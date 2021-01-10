#r "nuget: Newtonsoft.Json"

namespace CommonUtils

open System
open System.Text.RegularExpressions
open Newtonsoft.Json


[<AllowNullLiteral>]
type Tweet [<JsonConstructor>] (myId:Guid,userId:Guid,message:String,tweetRef:Guid) =
    let timestamp = DateTime.Now
    let mentions =
        let matches = Regex.Matches (message, @"\B@\w+")
        Array.init matches.Count  (fun index -> matches.[index].ToString().Substring(1)) 
    let hashTags = 
        let matches = Regex.Matches (message, @"\B#\w+")
        Array.init matches.Count  (fun index -> matches.[index].ToString())   
    new (myId:Guid,userId:Guid,message:String) = Tweet(myId,userId,message,Guid.Empty)
    new (myId:Guid,userId:Guid,tweetRef:Guid) = Tweet(myId,userId,"",Guid.Empty)
    member this.ID = myId
    member this.UserID = userId 
    member this.Message = message
    member this.TimeStamp = timestamp
    member this.Mentions = mentions
    member this.HashTags = hashTags 
    member this.IsReTweet = (tweetRef<>Guid.Empty) 
    member this.Tweetref = tweetRef 
    member this.Print () = 
        // if not this.IsReTweet then printfn "Tweet ID: %A   User ID: %A     message: %A      timestamp: %A" myId userId message timestamp
        // else printfn "Tweet ID: %A   User ID: %A     Retweet REF: %A        timestamp: %A" myId userId tweetRef timestamp
        if this.IsReTweet then printfn "ReTweet: ref-%A" tweetRef
        else printfn "%s" message

type ServerCommand =
    | Register of name:string * password:string
    | LoginRequest of userName:string * password:string
    | LogoutRequest of exitFlag:bool
    | TweetRequest of message:string
    | ReTweetRequest of tweetID:Guid
    | FollowRequest of refName:string
    | QueryRequest
    | HashTagQueryRequest of hashTag:string
    | MentionQueryRequest of name:string
    | PrintRandomUser
    | PrintUser of name:string
    | MyDetailRequest
    | PingRequest
    | Exit 

type ClientCommand =
    | RegisterAcknowledgement of userID:Guid
    | LoginAcknowledgement of userID:Guid
    | LogoutAcknowledgement of exitFlag:bool
    | TweetAcknowledgement
    | TweetNotification of Tweet
    | QueryResults of tweets:Tweet[] * queryID:Guid * chunkLength:int * chunkIndex:int
    | MyDetails of userID:Guid * name:string * following:string[] * followers:string[] 
    | PrintYourDetails
    | PrintLiveNotifications
    | PrintNotifications
    | PrintQueryResults
    | TriggerTweet of message:string
    | TriggerFollow of refName:string
    | TriggerRegister of name:string * password:string
    | TriggerLogIn of name:string * password:string
    | TriggerLogOut of exitFlag:bool
    | TriggerQuery
    | TriggerHashTagQuery of hashTag:string
    | TriggerMentionQuery
    | TriggerPing
    | PongResponse
    | ErrorResponse of message:string
    | SuccessResponse of message:string

type SimulatorCommand =
    | LoginSuccessfull of userName:string
    | LogOutSuccessfull of userName:string
    | QueryResponseAck of userName:string
    | GeneralAcknowledgment
    | ErrorReport of message:string
    | SuccessReport of message:string


type ClientRequest (sessionID:Guid,userID:Guid,command:ServerCommand) =
    member this.SessionID = sessionID
    member this.UserID = userID
    member this.Command = command

type ServerResponse (sessionID:Guid,command:ClientCommand) =
    member this.SessionID = sessionID
    member this.Command = command

type ServerRequest (sessionID:Guid,command:ClientCommand) =
    member this.SessionID = sessionID
    member this.Command = command

type ClientResponse (sessionID:Guid,command:ServerCommand) =
    member this.SessionID = sessionID
    member this.Command = command