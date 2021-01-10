#r "nuget: Newtonsoft.Json"
#r "nuget: Suave"

#if INTERACTIVE
#load "serverUtils.fsx"
#endif

open CommonUtils
open ServerUtils
open System
open Suave
open Suave.Operators
open Suave.Filters
open Suave.RequestErrors
open Suave.Logging
open Newtonsoft.Json
open Suave.Sockets
open Suave.Sockets.Control
open Suave.WebSocket
open Suave.Writers
open Suave.Successful


let silent = if fsi.CommandLineArgs.Length > 1 then (Convert.ToBoolean fsi.CommandLineArgs.[1]) else false

let sessionStore = SessionStore()

let dataStore= DataStore()

let sendToUser(userID:Guid,command:ClientCommand) = socket {
    let (sessionID,webSocket) = sessionStore.GetSocket(userID:Guid)
    let serverResponse = ServerResponse(sessionID,command)
    let jsonResponse = JsonConvert.SerializeObject(serverResponse)
    let byteResponse = jsonResponse |> System.Text.Encoding.ASCII.GetBytes |> ByteSegment
    do! webSocket.send Text byteResponse true
}

let register(name:string,password:string) =
    if dataStore.HasUser(name) then
        Guid.Empty
    else 
        let guid = Guid.NewGuid()
        let user = User(guid,name)
        dataStore.PutUser(user)
        dataStore.AddCred(name,password)
        guid

let processTweet (userID:Guid,reTweetRef:Guid,message:string) =
    let tweetID = Guid.NewGuid()
    let tweet = if reTweetRef=Guid.Empty then Tweet(tweetID,userID,message) else Tweet(tweetID,userID,reTweetRef)
    dataStore.PutTweet(tweet) |> ignore
    for hashTag in tweet.HashTags do
        dataStore.AddHashTag(hashTag,tweetID)
    for mention in tweet.Mentions do
        dataStore.AddMention(mention,tweetID)
    let _,user = dataStore.GetUser(userID)
    for followerID in user.Followers do
        if dataStore.IsConnected(followerID) then
            sendToUser(followerID,TweetNotification(tweet)) |> Async.RunSynchronously |> ignore

let tweet (userID:Guid,message:string) = 
    processTweet(userID,Guid.Empty,message)

let reTweet (userID:Guid,tweetRef:Guid) = processTweet(userID,tweetRef,"")

let follw (userID:Guid,refName:string) =
    let (valid,refID) = dataStore.GetUser(refName)
    if valid then
        let (_,user) = dataStore.GetUser(userID)
        user.Follow(refID)
        dataStore.UpdateUser(user) // todo check without put
        let (_,followedUser) = dataStore.GetUser(refID)
        followedUser.FollowsMe(userID)
        dataStore.UpdateUser(followedUser) // todo check without put
        true
    else false

let printUser(name:string) = 
        let _,userID = dataStore.GetUser(name)
        let _,user = dataStore.GetUser(userID)
        printfn "******************************"
        printfn "USER-  name:%s ID:%A" user.Name user.Id
        printfn "**** # Follows: %A" user.Following.Length
        printfn "**** # Followers: %A" user.Followers.Length
    
let printRandomUser() =
    let randomUser = dataStore.GetRandomUser()
    printfn "******************************"
    printfn "USER-  name:%s ID:%A" randomUser.Name randomUser.Id
    sendToUser (randomUser.Id,PrintNotifications) |> Async.RunSynchronously |> ignore

let maxResponseSize = 250
let sendQueryResult(userID:Guid,result:Tweet[]) =
    let qid = Guid.NewGuid()
    if result.Length <= maxResponseSize then
        sendToUser (userID,QueryResults(result,qid,1,0)) |> Async.RunSynchronously |> ignore
    else
        let chunks = Array.splitInto ((result.Length+maxResponseSize-1)/maxResponseSize) result
        let mutable i = 0
        for chunk in chunks do
            sendToUser (userID,QueryResults(chunk,qid,chunks.Length,i)) |> Async.RunSynchronously |> ignore
            i <- i+1

let queryTweets(userId:Guid) =
    let _,user =  dataStore.GetUser(userId)
    let result = dataStore.QueryTweets(user.Following)
    sendQueryResult(userId,result)

let queryHashTag(userId:Guid,hashTag:string) =
    let result = dataStore.QueryHashTag(hashTag)
    sendQueryResult(userId,result)

let queryMention(userId:Guid,mention:string) =
    let result = dataStore.QueryMention(mention)
    sendQueryResult(userId,result)

let login (userName:string,password:string) = 
    let isValid,pRef =dataStore.GetCred(userName)
    if isValid && password=pRef then
        let _,userID = dataStore.GetUser(userName)
        dataStore.MarkAsConnected(userID)
        true
    else false
let logout (userID:Guid) = dataStore.MarkAsDisConnected(userID)

let getUserName (userID:Guid) = 
    let _,user = dataStore.GetUser(userID)
    user.Name

let handleMyDetailRequest(userID:Guid) =
    let _,user = dataStore.GetUser(userID)
    let followerNames = Array.init user.Followers.Length (fun i -> getUserName(user.Followers.[i]))
    let followingNames = Array.init user.Following.Length (fun i -> getUserName(user.Following.[i]))
    sendToUser(userID, MyDetails (userID,user.Name,followingNames,followerNames)) |> Async.RunSynchronously |> ignore


let webSocketHandle (webSocket : WebSocket) (context: HttpContext) = socket {
    let mutable mySessionID:Guid = Guid.Empty
    let mutable user:User = null

    let sendResponse(command:ClientCommand) = socket {
        let serverResponse = ServerResponse(mySessionID,command)
        let jsonResponse = JsonConvert.SerializeObject(serverResponse)
        let byteResponse = jsonResponse |> System.Text.Encoding.ASCII.GetBytes |> ByteSegment
        do! webSocket.send Text byteResponse true
    }

    let sendErrorResponse(message:string) = socket {
        do! sendResponse(ErrorResponse(message))
    }

    let sendSuccessResponse(message:string) = socket {
        do! sendResponse(SuccessResponse(message))
    }

    let createNewSessionInfo(userID:Guid) =
        let isValid,userT = dataStore.GetUser(userID)
        if isValid then 
            let duplicate,oldID = sessionStore.GetID(userID)
            if duplicate then
                sessionStore.Remove(oldID,userID)
            mySessionID <- Guid.NewGuid()
            user <- userT
            sessionStore.Add (mySessionID,user.Id,webSocket)

    let isValidSession(sessionRef:Guid,userID:Guid) = 
        if isNull user || sessionRef <> mySessionID then false
        else (userID = user.Id)
        
    let closeSession() =
        if isNull user then 
            sessionStore.Remove(mySessionID,Guid.Empty)
        else
            sessionStore.Remove(mySessionID,user.Id)

    let mutable loop = true

    let validateAndHandle(sessionID:Guid,userID:Guid,command:ServerCommand) = socket {
        if sessionID <> mySessionID || isNull user then
            do! sendErrorResponse("invalid session: session-id provided not valid")
        else if userID <> user.Id then
            do! sendErrorResponse("invalid session: user details invalid")
        else
            match command with
            | LoginRequest (userName,password) ->
                if login(userName,password) then 
                    do! sendResponse(LoginAcknowledgement userID)
                else 
                    do! sendErrorResponse("login failed: bad credentials")
            | LogoutRequest exitFlag-> 
                logout(userID)
                do! sendResponse(LogoutAcknowledgement exitFlag)
                if exitFlag then
                    loop <- false
                    Async.Sleep(5000) |> Async.RunSynchronously
            | TweetRequest (message) -> 
                tweet(userID,message)
                do! sendResponse(TweetAcknowledgement)
            | ReTweetRequest (tweetRef:Guid) ->
                reTweet(userID,tweetRef)
            | QueryRequest -> 
                queryTweets(userID)
            | HashTagQueryRequest (hashTag) -> 
                queryHashTag(userID,hashTag)
            | MentionQueryRequest (mention) -> 
                queryMention(userID,mention)
            | FollowRequest (refName:string) ->
                if follw(userID,refName) then do! sendSuccessResponse(sprintf "followed user:%s successfully" refName)
                else do! sendErrorResponse(sprintf "follow request failed: user:%s doesn't exist" refName)
            | MyDetailRequest -> handleMyDetailRequest(userID) 
            | PrintRandomUser -> 
                printRandomUser()
            | PrintUser name -> 
                printUser(name)
            | PingRequest ->
                do! sendResponse(PongResponse)
            | Register (name,password) -> 
                let id = register(name,password)
                if id <> Guid.Empty then
                   createNewSessionInfo(id) 
                   do! sendResponse((RegisterAcknowledgement id))
                else
                    do! sendErrorResponse("invalid request: user name already exists")
            | _ -> 
                printfn "server: unhandled request %A" command
                ()    
    }
   

    let emptyResponse = [||] |> ByteSegment

    while loop do
      let! msg = webSocket.read()
      match msg with
      | (Text, data, true) ->
        let jsonRequest = UTF8.toString data
        if not silent then
            printfn "Request from client %A" jsonRequest
        
        let clientRequest =  JsonConvert.DeserializeObject<ClientRequest> jsonRequest 

        if clientRequest.SessionID <> Guid.Empty then
            do! validateAndHandle(clientRequest.SessionID,clientRequest.UserID,clientRequest.Command)
        else
            match clientRequest.Command with
            | Register (name,password) -> 
                // printfn "user: %s wants to register!" name
                let id = register(name,password)
                if id <> Guid.Empty then
                   createNewSessionInfo(id) 
                   do! sendResponse((RegisterAcknowledgement id))
                else
                    do! sendErrorResponse("invalid request: user name already exists")
            | LoginRequest (userName,password) ->
                if login(userName,password) then
                    let _,userID = dataStore.GetUser(userName) 
                    createNewSessionInfo(userID)
                    do! sendResponse(LoginAcknowledgement userID)
                else 
                    do! sendErrorResponse("login failed: bad credentials")                
            | _ -> 
                printfn "unexpected request: %A" clientRequest
                do! sendErrorResponse("invalid session: session-id is empty")
      | (Close, _, _) ->
        printfn "client is closing!"
        closeSession()
        do! webSocket.send Close emptyResponse true   
        loop <- false
      | _ -> 
            printfn "server socket handle: unexpected message: %A" msg    
            ()

    }


let mutable httpSessionStore = SessionStore()


let createNewSession(userID:Guid) =
    let duplicate,oldID = httpSessionStore.GetID(userID)
    if duplicate then
        httpSessionStore.Remove(oldID,userID)
    let sessionID = Guid.NewGuid()
    httpSessionStore.Add (sessionID,userID)
    sessionID

let genResponse(sessionID:Guid,command:ClientCommand) =
        let serverResponse = ServerResponse(sessionID,command)
        let jsonResponse = JsonConvert.SerializeObject(serverResponse)
        OK(jsonResponse)

let genSuccessResponse(sessionID:Guid,message:string) =
    genResponse(sessionID,SuccessResponse(message))

let genEmptyResponse(sessionID:Guid) = genSuccessResponse(sessionID,"")

let httpHandle (context: HttpContext) =   
    let jsonRequest = UTF8.toString (context.request.rawForm)
    printfn "Request from client %A" jsonRequest
    let clientRequest =  JsonConvert.DeserializeObject<ClientRequest> jsonRequest 

    if clientRequest.SessionID = Guid.Empty then
        match clientRequest.Command with
        | Register (name,password) -> 
            let id = register(name,password)
            if id <> Guid.Empty then
               let sessionId = createNewSession(id) 
               genResponse(sessionId,(RegisterAcknowledgement id))
            else
                BAD_REQUEST("invalid request: user name already exists")
        | LoginRequest (userName,password) ->
            let _,userID = dataStore.GetUser(userName) 
            if clientRequest.UserID = Guid.Empty  || clientRequest.UserID <> userID then
                BAD_REQUEST("invalid request: user-id is invalid")
            else if login(userName,password) then
                let sessionId = createNewSession(userID) 
                genResponse(sessionId,(LoginAcknowledgement userID))
            else 
                UNAUTHORIZED("login failed: bad credentials")                
        | _ -> 
            printfn "unexpected request: %A" clientRequest
            UNAUTHORIZED("invalid session: session-id is empty")
    else if clientRequest.UserID = Guid.Empty then
                BAD_REQUEST("invalid request: user-id is invalid")
    else
        let valid,sessionRef = httpSessionStore.GetID(clientRequest.UserID)
        if not valid || clientRequest.SessionID <> sessionRef then
            BAD_REQUEST("invalid session: session-id provided not valid")
        else
            let userID = clientRequest.UserID
            let sessionID = clientRequest.SessionID
            match clientRequest.Command with
            | LoginRequest (userName,password) ->
                if login(userName,password) then 
                    genResponse(sessionID,(LoginAcknowledgement userID))
                else 
                    UNAUTHORIZED("login failed: bad credentials")
            | LogoutRequest exitFlag-> 
                logout(userID)
                genResponse(sessionID,(LogoutAcknowledgement exitFlag))            
            | TweetRequest (message) -> 
                tweet(userID,message)
                genResponse(sessionID,TweetAcknowledgement)            
            | ReTweetRequest (tweetRef:Guid) ->
                reTweet(userID,tweetRef)
                genEmptyResponse(sessionID)
            | QueryRequest -> 
                queryTweets(userID)
                genEmptyResponse(sessionID)
            | HashTagQueryRequest (hashTag) -> 
                queryHashTag(userID,hashTag)
                genEmptyResponse(sessionID)
            | MentionQueryRequest (mention) -> 
                queryMention(userID,mention)
                genEmptyResponse(sessionID)
            | FollowRequest (refName:string) ->
                if follw(userID,refName) then 
                    genSuccessResponse(sessionID,sprintf "followed user:%s successfully" refName)
                else BAD_REQUEST(sprintf "follow request failed: user:%s doesn't exist" refName)
            | MyDetailRequest -> 
                handleMyDetailRequest(userID) 
                genEmptyResponse(sessionID)
            | PrintRandomUser -> 
                printRandomUser()
                genEmptyResponse(sessionID)
            | PrintUser name -> 
                printUser(name)
                genEmptyResponse(sessionID)
            | PingRequest ->
                genResponse(sessionID,PongResponse)
            | Register (name,password) -> 
                let id = register(name,password)
                if id <> Guid.Empty then
                   let newSeesionID=createNewSession(id) 
                   genResponse(sessionID,(RegisterAcknowledgement id))
                else
                    BAD_REQUEST("invalid request: user name already exists")
            | _ -> 
                printfn "server: unhandled request %A" (clientRequest.Command)
                BAD_REQUEST("invalid commmand!")


printfn "starting server"

let app : WebPart = 
  choose [
    path "/websocket" >=> handShake webSocketHandle
    path "/api/" >=> choose [
      POST >=> context httpHandle
    ]
    NOT_FOUND "Found no handlers." 
    ]

let config =
  { defaultConfig with
      bindings = [ HttpBinding.createSimple HTTP "127.0.0.1" 8082 ]
    }

startWebServer { config with logger = Targets.create Verbose [||] } app
