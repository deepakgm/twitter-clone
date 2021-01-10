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
            | SuccessResponse _ -> 
                controllerRef <! GeneralAcknowledgment
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
    let clientRef = spawn mailbox.Context.System "client1"  (client (0.0,controllerRef,false))
    let mutable errorFlag = false

    let waitForResponse() = actor {
        errorFlag <- false
        let! response = mailbox.Receive()
        match response with
        | GeneralAcknowledgment -> 
            printfn "Operation successful"
        | ErrorReport message -> 
            printfn "Error while operation: %s \n Try again.." message
            errorFlag <- true
        | _ -> ()
    }

    let rec initOp () = 
        actor {
            printfn "*******\nEnter a number to choose operation: \n 0 to Register\n 1 to Login"
            let op = Console.ReadLine() 
            match op with 
            | "0" -> return! register ()
            | "1" -> return! login ()
            | _ ->
                printfn "invalid option:%s" op
                return! initOp()
        } 
    and register() = 
        actor {
            printfn "Enter user name:"
            let userName = Console.ReadLine().Trim()
            printfn "Enter password:"
            let password = Console.ReadLine().Trim()
            printfn "registering with user-name:%s password:%s" userName password
            clientRef <! TriggerRegister (userName,password)
            let! _ = waitForResponse()
            return! initOp()
        }
    and login() = 
        actor {
            printfn "Enter user name:"
            let userName = Console.ReadLine().Trim()
            printfn "Enter password:"
            let password = Console.ReadLine().Trim()
            printfn "logging in with user-name:%s password:%s" userName password
            clientRef <! TriggerLogIn (userName,password)
            let! _ = waitForResponse()
            if errorFlag then return! initOp()
            else return! mainOp()
        }
    and mainOp () = 
        actor {
            printfn "*******\nEnter a number to choose operation: \n 0 to Print My Details\n 1 to Logout\n 2 to Follow\n 3 to Tweet\n 4 to Search for subscribed tweets\n 5 to Search for tweets with my mentions\n 6 to Search for tweets with hashtag\n 7 to Show existing notifications\n 8 to Show live notifications"
            let op = Console.ReadLine() 
            match op with 
            | "0" -> return! myDetails ()
            | "1" -> return! logout ()
            | "2" -> return! follow ()
            | "3" -> return! tweet ()
            | "4" -> return! querySubscribedTweets ()
            | "5" -> return! queryMyMentions ()
            | "6" -> return! queryHashTags ()
            | "7" -> return! printNotifications ()
            | "8" -> return! pollForLiveNotifications ()
            | _ ->
                printfn "invalid option:%s" op
                return! initOp()
        } 
    and myDetails() =
        actor {
            printfn "Searching details.."
            clientRef <! PrintYourDetails
            Async.Sleep(1000) |> Async.RunSynchronously
            return! mainOp()   
        }
    and logout() =
        actor {
            printfn "Logging out.."
            clientRef <! TriggerLogOut false
            let! _ = waitForResponse()
            return! initOp()   
        }
    and follow() =
        actor {
            printfn "Who you want to follow?"
            let name =  Console.ReadLine()             
            clientRef <! TriggerFollow (name)
            let! _ = waitForResponse()
            return! mainOp()   
        }
    and tweet() =
        actor {
            printfn "Enter the tweet to be sent (in single line):"
            let message =  Console.ReadLine()             
            clientRef <! TriggerTweet (message)
            let! _ = waitForResponse()
            return! mainOp()   
        }
    and querySubscribedTweets() =
        actor {
            printfn "Searching tweets.."
            clientRef <! TriggerQuery
            let! _ = waitForResponse()
            Async.Sleep(500) |> Async.RunSynchronously
            clientRef <! PrintQueryResults
            Async.Sleep(500) |> Async.RunSynchronously
            return! mainOp()   
        }
    and queryMyMentions() =
        actor {
            printfn "Searching tweets with my mentions.."
            clientRef <! TriggerMentionQuery
            let! _ = waitForResponse()
            Async.Sleep(500) |> Async.RunSynchronously
            clientRef <! PrintQueryResults
            Async.Sleep(500) |> Async.RunSynchronously
            return! mainOp()   
        }
    and queryHashTags() =
        actor {
            printfn "Enter the hash-tag to be serched (like #peace):"
            let hashTag =  Console.ReadLine()                 
            clientRef <! TriggerHashTagQuery (hashTag)
            let! _ = waitForResponse()
            Async.Sleep(500) |> Async.RunSynchronously
            clientRef <! PrintQueryResults
            Async.Sleep(500) |> Async.RunSynchronously
            return! mainOp()   
        }
    and printNotifications() =
        actor {
            printfn "Checking notifications.."
            clientRef <! PrintNotifications
            Async.Sleep(1000) |> Async.RunSynchronously
            return! mainOp()   
        }
    and pollForLiveNotifications() =
        actor {
            printfn "****************************************"
            printfn "Starting to poll for live notifcations.."
            printfn "****************************************"
            clientRef <! PrintLiveNotifications
        }
    let! _ = initOp()
    ()
}

let myActorSystem = System.create "myActorSystem" (Configuration.load ())

printfn "starting client.."
spawn myActorSystem "controller" controller |> ignore

myActorSystem.WhenTerminated.Wait ()
