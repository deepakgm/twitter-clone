#r "nuget: Akka.FSharp" 
#r "nuget: Newtonsoft.Json"
#r "nuget: WebSocketSharp, 1.0.3-rc11"
#r "nuget: MathNet.Numerics" 

#if INTERACTIVE
#load "clientUtils.fsx"
#endif

open Akka.FSharp
open WebSocketSharp
open System
open System.Threading
open System.Diagnostics
open System.Collections.Generic
open Newtonsoft.Json
open MathNet.Numerics.Distributions;
open ClientUtils
open CommonUtils


let SERVER_SOCKET_URL = "ws://127.0.0.1:8082/websocket" 


let client (reTweetProb:double,controllerRef:Akka.Actor.ActorSelection,silent:bool) (mailbox:Actor<_>)  = 
    let mutable name = ""
    let mutable password = ""
    let mutable myID:Guid = Guid.Empty
    let mutable mySessionID:Guid = Guid.Empty
    let random = Random()
    let tweetNotifications = TweetNotificationStore()
    let queryResult = QueryResult()
    let ws = new WebSocket(SERVER_SOCKET_URL)

    // ws.OnOpen.Add(fun args -> System.Console.WriteLine("Open"))
    // ws.OnClose.Add(fun args -> System.Console.WriteLine("Close"))
    ws.OnMessage.Add(fun args -> 
        let jsonResponse = args.Data
        if not silent then
            printfn "Response from server: %A" jsonResponse
        let serverResponse =  JsonConvert.DeserializeObject<ServerResponse> jsonResponse
        mySessionID <- serverResponse.SessionID
        mailbox.Self <! serverResponse.Command
        )
    ws.OnError.Add(fun args -> System.Console.WriteLine("Error: {0}", args.Message))

    ws.Connect()

    let sendToServer(command:ServerCommand) = 
        let request = ClientRequest(mySessionID,myID,command)
        let jsonRequest = JsonConvert.SerializeObject (request)
        if not silent then printfn "Request to Sever: %s" jsonRequest
        ws.Send(jsonRequest)

    let printMyDetails(following:string[],followers:string[]) =
        printfn "************ My details ************"
        printfn "Client ID: %A  Name: %s" myID name
        printfn "# following:%i     # followers:%i" following.Length followers.Length
        if following.Length <> 0 then 
            printfn "Followings:    "
            for userName in following do printf "%s  " userName
        if followers.Length <> 0 then 
            printfn "\nFollowers:   "
            for userName in followers do printf "%s  " userName
        printfn "\n"

    let mutable printLiveNotifications = false    
    let tweetHandle(tweet:Tweet) =
        tweetNotifications.Add(tweet)
        if printLiveNotifications then
            printfn "Tweet Notification:"
            tweet.Print()
        // retweet with probability
        if random.NextDouble() < reTweetProb then
            sendToServer(ReTweetRequest (tweet.ID))
    
    let queryResultHandle(msg) =
        match msg with
        | QueryResults (tweets, queryID, chunkLength, chunkIndex) ->
            if queryResult.ID <> queryID then
                queryResult.SetID(queryID)
            queryResult.Add(tweets)
            if (chunkIndex+1) = chunkLength then
                controllerRef <! GeneralAcknowledgment 
        | _ -> () 


    let rec initSate () = 
        actor {
            let! msg = mailbox.Receive()
            match msg with
            | TriggerRegister (nameT,passwordT) ->
                name <- nameT
                password <- passwordT
                sendToServer( (Register (name,password)))
                return! initSate()
            | RegisterAcknowledgement id -> 
                myID <- id
                controllerRef <! GeneralAcknowledgment
                return! initSate()
            | TriggerLogIn (nameT,passwordT) -> 
                name <- nameT
                password <- passwordT
                sendToServer(LoginRequest (name,password))
                return! loginState ()
            | ErrorResponse message ->
                controllerRef <! ErrorReport message
                return! initSate()
            | _ -> 
            return! initSate()
        } 
    and loginState () = 
        actor {
            let! msg = mailbox.Receive()
            match msg with
            | LoginAcknowledgement userID-> 
                myID <- userID
                controllerRef <! GeneralAcknowledgment
                return! activeState ()
            | ErrorResponse message ->
                controllerRef <! ErrorReport message
                return! initSate()
            | _ -> ()
            return! loginState()  
        }
    and activeState () = 
        actor {
            let! msg = mailbox.Receive()
            match msg with
            | TweetNotification tweet -> 
                tweetHandle(tweet)
                return! activeState()
            | TweetAcknowledgement -> 
                controllerRef <! GeneralAcknowledgment
                return! activeState()
            | TriggerTweet messsage ->
                sendToServer(TweetRequest(messsage))
                return! activeState()
            | TriggerFollow refName ->
                sendToServer (FollowRequest(refName))
                return! activeState()
            | TriggerQuery -> 
                sendToServer(QueryRequest)
                return! activeState()
            | TriggerHashTagQuery hashTag -> 
                sendToServer(HashTagQueryRequest(hashTag))
                return! activeState()
            | TriggerMentionQuery -> 
                sendToServer(MentionQueryRequest(name))
                return! activeState()
            | QueryResults _ -> 
                queryResultHandle (msg)
                return! activeState()
            | TriggerLogOut exitFlag -> 
                sendToServer(LogoutRequest exitFlag)
                return! logOutSate ()
            | TriggerPing -> 
                sendToServer(PingRequest)
                return! activeState()
            | PongResponse -> 
                controllerRef <! GeneralAcknowledgment
                return! activeState()
            | PrintNotifications -> 
                tweetNotifications.Print()
                return! activeState()
            | PrintLiveNotifications -> 
                printLiveNotifications <- true
                return! activeState()
            | MyDetails (_,_,following,followers) ->  
                printMyDetails(following,followers)
                return! activeState()
            | PrintYourDetails -> 
                sendToServer(MyDetailRequest)
                return! activeState()
            | PrintQueryResults -> 
                queryResult.Print()
                return! activeState()
            | ErrorResponse message ->
                controllerRef <! ErrorReport message
                return! activeState()
            | _ ->  ()
                // printfn "client-%s: unexpected msg %A" name msg 
            return! activeState  ()
        }    
    and logOutSate () =
        actor {
            let! msg = mailbox.Receive()
            match msg with
            | LogoutAcknowledgement exitFlag -> 
                controllerRef <! GeneralAcknowledgment
                if not exitFlag then 
                    return! logOutSate ()
            | TriggerLogIn (nameT,passwordT) -> 
                name <- nameT
                password <- passwordT 
                sendToServer(LoginRequest (name,password))
                return! loginState ()
            | ErrorResponse message ->
                controllerRef <! ErrorReport message
                return! initSate()
            | _ -> return! loginState()
        }
    initSate ()

let controller (mailbox:Actor<_>) = actor {
    let controllerRef = select "/user/controller" mailbox.Context.System

    let clientRef = spawn mailbox.Context.System "client"  (client (0.0,controllerRef,false))
    clientRef <! TriggerRegister ("deepak","p")
    let! _ = mailbox.Receive()
    printfn "reg done"
    clientRef <! TriggerLogIn ("deepak","p")
    let! m = mailbox.Receive()
    printfn "login done %A" m
    // clientRef <! TriggerLogOut
    // let! m = mailbox.Receive()
    // printfn "logout done %A" m

    let clientRef2 = spawn mailbox.Context.System "client2"  (client (0.0,controllerRef,false))
    clientRef2 <! TriggerRegister ("deepak2","p")
    let! _ = mailbox.Receive()
    printfn "reg2 done"
    clientRef2 <! TriggerLogIn ("deepak2","p")
    let! m = mailbox.Receive()
    printfn "login2 done %A" m
    clientRef2 <! TriggerFollow "deepak"

    clientRef <! TriggerTweet ("some message @deepak2 #all")
    let! m = mailbox.Receive()
    printfn "sent some tweet %A" m
    // clientRef2 <! TriggerMentionQuery
    // let! _ = mailbox.Receive()
    // clientRef2 <! PrintQueryResults
    // clientRef2 <! TriggerQuery
    // let! _ = mailbox.Receive()
    // clientRef2 <! PrintQueryResults
    // Thread.Sleep(1000)
    printfn "hasha tag query"
    clientRef <! TriggerHashTagQuery ("#all")
    let! _ = mailbox.Receive()
    clientRef <! PrintQueryResults
    ()
}


let random = Random() 

let getNRandomNumOtherThanSelf(self:int,limit:int,n:int) =
        let mutable list = []
        for i in 1..n do
            let mutable num = random.Next(limit)+1
            while List.exists ((=) num) list || num = self do
                num <- random.Next(limit)+1
            list <- num::list
        list


let messages = [|"i like coffe #coffelovers" ;"fsharp is great"; "please were mask";"election resutls are out! #election2020"; "Never say never"; "God does not play dice with the universe"|] 
let hashTags = [|"#COP5615isgreat" ; "#coolFsharp"; "#peace"; "#Covid19"; "#Akka"; "#dotnet"; "#GoGators"; "#asyncWithoutBoundary"|] 
let randomMessage () = messages.[random.Next(messages.Length)]
let randomHashTags () = 
    let mutable hs = ""
    let count = random.Next(3)
    for i in getNRandomNumOtherThanSelf (0,hashTags.Length,count) do
        hs <- sprintf "%s %s" hs hashTags.[i-1]
    hs
let randomMentions (self:int,n:int) = 
    let mutable mentions = ""
    let count = random.Next(3)
    for i in getNRandomNumOtherThanSelf (self,n,count) do
        mentions <- (sprintf "%s @user%i" mentions i)
    mentions

let simulator (n:int) (reTweetProb:double) (dynamicTweets:bool) (disconnectPerc:double) (mailbox:Actor<_>) =  actor {
    let controllerRef = select "/user/simulator" mailbox.Context.System
    let userRef i = select (sprintf "/user/client-user%i" i) mailbox.Context.System
    
    let zipfSkew = 1.5 

    let connectionMap = Dictionary<int,bool>(n)

    let rec waitTillReply (i:int,max:int) = actor {
        if max > 0 then
            let! msg = mailbox.Receive()
            match msg with
            | GeneralAcknowledgment -> 
                if (i+1 < max) then
                    return! waitTillReply(i+1,max)
            | _ -> return! waitTillReply(i,max)
    }
   
    let initClientActors() = actor {
        for index in 1..n do
            let name = sprintf "user%i" index
            spawn mailbox.Context.System (sprintf "client-%s" name) (client (reTweetProb,controllerRef,true)) |> ignore
    }

    let register () = actor {
        let timer = Stopwatch.StartNew()
        printfn "*********** Starting User Registration ***********"
        for index in 1..n do
            (userRef index) <! TriggerRegister ((sprintf "user%i" index),"simulator")
        let! _ =  waitTillReply (0,n)
        printfn "*********** User Registration Finished ***********"
        timer.Stop()
        printfn "time taken = %dms" timer.ElapsedMilliseconds
    }

    let login () = actor {
        let timer = Stopwatch.StartNew()
        printfn "*********** Starting User Login ***********"
        for index in 1..n do
            (userRef index) <! TriggerLogIn ((sprintf "user%i" index),"simulator")
            connectionMap.Add(index,true)
        let! _ =  waitTillReply (0,n)
        printfn "*********** User Login Finished ***********"
        timer.Stop()
        printfn "time taken = %dms" timer.ElapsedMilliseconds
    }

    let disconnectUsers () = actor {
        let numClientsToBeDisconnected = (double n) * disconnectPerc |> int
        printfn "*********** Starting to disconnect %i users ***********" numClientsToBeDisconnected
        for index in getNRandomNumOtherThanSelf(0,n,numClientsToBeDisconnected) do
            (userRef index) <! TriggerLogOut false
            connectionMap.[index] <- false
        let! _ = waitTillReply(0,numClientsToBeDisconnected)
        printfn "*********** Disconnect completed ***********"
    }

    let reConnectUsers () = actor {
        let numClientsToBeReConnected = (double n) * disconnectPerc |> int
        printfn "*********** Starting to reconnect %i users ***********" numClientsToBeReConnected
        let list = new List<int>(numClientsToBeReConnected)
        for key in connectionMap.Keys do
            if not connectionMap.[key] then
                (userRef key) <! TriggerLogIn
                list.Add(key)
        for key in list do connectionMap.[key] <- true
        let! _ = waitTillReply(0,numClientsToBeReConnected)
        printfn "*********** Reconnect completed ***********"
    }

    let tweet () = actor {
        let tweetsPerUser = 10
        let timer = Stopwatch.StartNew()
        let zipf = new Zipf(zipfSkew,n)
        let mutable totalNumberOfTweets = 0
        printfn "*********** Starting to Tweet ***********"
        for index in 1..n do
            if connectionMap.[index] then
                let numOfTweets = 
                    if dynamicTweets then ((double (n-index))/(double n)) * 20.0 |> int else tweetsPerUser
                totalNumberOfTweets <- totalNumberOfTweets + numOfTweets
                for k in 1..numOfTweets do
                    (userRef index) <! TriggerTweet (sprintf "%s%s%s" (randomMessage()) (randomMentions(index,n)) (randomHashTags()))
        printfn "total number of tweets sent: %i" totalNumberOfTweets
        let! _ =  waitTillReply (0,totalNumberOfTweets)
        printfn "*********** Tweeting Finished ***********"
        timer.Stop()
        printfn "time taken = %dms" timer.ElapsedMilliseconds
    }

    // subscribe to each other to form zipf disrtibution
    let subscribe () = actor {
        let averageNumberFollowers = 1 
        let zipf = new Zipf(zipfSkew,n*averageNumberFollowers)
        let timer = Stopwatch.StartNew()
        printfn "*********** Starting to subscribe ***********"
        for i in 1..n do
            let name = sprintf "user%i" i
            let followerCount = (zipf.Probability(i) * (double) n) |> int
            for index in getNRandomNumOtherThanSelf(i,n,followerCount) do
                (userRef index) <! TriggerFollow name
        (userRef 1) <! TriggerPing
        let! _ =  waitTillReply (0,1)
        printfn "*********** Subscribe Finished ***********"
        timer.Stop()
        printfn "time taken = %dms" timer.ElapsedMilliseconds
    }

    let performHashTagQueries() = actor {
        let timer = Stopwatch.StartNew()
        printfn "*********** Starting to Query HashTag  ***********"
        for i in 1..n do
            (userRef i) <! TriggerHashTagQuery "#coolFsharp"
        let! _ =  waitTillReply (0,n)
        printfn "*********** HashTags querries Finished ***********"
        timer.Stop()
        printfn "time taken = %dms" timer.ElapsedMilliseconds
    }

    let performMyMentionQueries() = actor {
        let timer = Stopwatch.StartNew()
        printfn "*********** Starting to Query My-Mentions  ***********"
        for i in 1..n do
            (userRef i) <! TriggerMentionQuery
        let! _ =  waitTillReply (0,n)
        printfn "*********** My-Mention querries Finished ***********"
        timer.Stop()
        printfn "time taken = %dms" timer.ElapsedMilliseconds
    }

    let performQueriesOnSuscribedTweets() = actor {
        let timer = Stopwatch.StartNew()
        printfn "*********** Starting to Query on Suscribed Tweets  ***********"
        for i in 1..n do
            (userRef i) <! TriggerQuery 
        let! _ =  waitTillReply (0,n)
        printfn "*********** Suscribed Tweets querries Finished ***********"
        timer.Stop()
        printfn "time taken = %dms" timer.ElapsedMilliseconds
    }

    let performQueries() = actor {
        let! _ = performHashTagQueries()  
        let! _ = performMyMentionQueries()     
        let! _ = performQueriesOnSuscribedTweets()     
        ()  
    }

    let logout () = actor {
        let timer = Stopwatch.StartNew()
        printfn "*********** Starting User Logout ***********"
        for index in 1..n do
            (userRef index) <! TriggerLogOut false
            connectionMap.[index] <- false
        let! _ =  waitTillReply (0,n)
        printfn "*********** User Logout Finished ***********"
        timer.Stop()
        printfn "time taken = %dms" timer.ElapsedMilliseconds           
    }
    
    let! _ = initClientActors()
    let! _ = register()
    let! _ = login()
    let! _ = subscribe()
    // let! _ = disconnectUsers()
    let! _ = tweet()
    // let! _ = reConnectUsers()
    // let! _ = performQueries() 
    let! _ = logout()
   
    printfn "*** Simulation finished ***"
    Thread.Sleep(2000)
    Async.AwaitTask(mailbox.Context.System.Terminate()) |> ignore 
}

let myActorSystem = System.create "myActorSystem" (Configuration.load ())

printfn "starting simulator.."
let n = if fsi.CommandLineArgs.Length > 1 then  (int fsi.CommandLineArgs.[1]) else 10
let reTweetProb = if fsi.CommandLineArgs.Length > 2 then (double fsi.CommandLineArgs.[2]) else 0.0
let dynamicTweets = if fsi.CommandLineArgs.Length > 3 then (Convert.ToBoolean fsi.CommandLineArgs.[3]) else false
let disconnectPerc = if fsi.CommandLineArgs.Length > 4 then (double fsi.CommandLineArgs.[4]) else 0.0
spawn myActorSystem "simulator" (simulator n reTweetProb dynamicTweets disconnectPerc) |> ignore

myActorSystem.WhenTerminated.Wait ()
