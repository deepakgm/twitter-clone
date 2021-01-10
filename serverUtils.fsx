#r "nuget: Suave"

#if INTERACTIVE
#load "commonUtils.fsx"
#endif

namespace ServerUtils

open System
open System.Threading
open System.Data
open System.Collections.Generic
open CommonUtils
open System.Collections.Concurrent
open Suave
open Suave.WebSocket

type SessionStore () =
    let sessions = new ConcurrentDictionary<Guid,WebSocket>()
    let sessionIDMap = new ConcurrentDictionary<Guid,Guid>()
    member this.Add(sessionID:Guid,userID:Guid,ws:WebSocket) = 
        sessions.TryAdd(sessionID,ws) |> ignore
        sessionIDMap.TryAdd(userID,sessionID) |> ignore
    member this.Add(sessionID:Guid,userID:Guid) = 
        sessionIDMap.TryAdd(userID,sessionID) |> ignore
    member this.Remove(sessionID:Guid,userID:Guid) =
        sessions.TryRemove(sessionID) |> ignore
        sessionIDMap.TryRemove(userID) |> ignore
    member this.GetID(userID:Guid) = 
        sessionIDMap.TryGetValue(userID)
    member this.GetSocket(userID:Guid) = 
        let sessionID = sessionIDMap.[userID]
        (sessionID,sessions.[sessionID])

[<AllowNullLiteral>]
type User (id1:Guid,name1:String) =
    let myId = id1
    let name = name1
    let mutable following:Guid[] = Array.empty
    let mutable followers:Guid[] = Array.empty
    member this.Id = myId
    member this.Name = name
    member this.Following = following
    member this.Followers = followers
    member this.Follow (id:Guid) = 
        following <- (Array.append following [|id|])
    member this.FollowsMe (id:Guid) = 
        followers <- (Array.append followers [|id|])

type TweetStore () =
    let lock = new ReaderWriterLockSlim();
    let table = new DataTable()
    let init () =
       table.Columns.Add("tweet_id",typeof<string>) |> ignore
       table.Columns.Add("user_id",typeof<string>) |> ignore
       table.Columns.Add("time_stamp",typeof<DateTime>) |> ignore
       ()
    do init ()
    member this.Add(tweet:Tweet) =
        lock.EnterWriteLock()
        table.Rows.Add(tweet.ID.ToString(),tweet.UserID.ToString(),tweet.TimeStamp) |> ignore
        lock.ExitWriteLock()
    member this.Query(fromUserIDs:Guid[]) = 
        let strArray = Array.init (fromUserIDs.Length) (fun i -> sprintf "'%s'" (fromUserIDs.[i].ToString()))
        let expression = sprintf "user_id in (%s)" (String.Join(",",strArray))
        // printfn "expression: %s" expression
        let result = table.Select(expression,"time_stamp")
        //set limited output
        let limit = if result.Length > 100 then 100 else result.Length
        Array.init limit (fun i ->  Guid(result.[i].["tweet_id"].ToString()))
    member this.Size () = table.Select().LongLength
        
type DataStore () =
    let users = ConcurrentDictionary<Guid,User>()
    let nameMap = ConcurrentDictionary<string,Guid>()
    let tweets = ConcurrentDictionary<Guid,Tweet>()
    let connectionMap = ConcurrentDictionary<Guid,bool> ()
    let hashTags = ConcurrentDictionary<string,List<Guid>> ()
    let mentions = ConcurrentDictionary<string,List<Guid>> ()
    let tweetStore = TweetStore()
    let creds = ConcurrentDictionary<string,string>()
    member this.GetUser (id:Guid) = users.TryGetValue(id) 
    member this.GetRandomUser() = users.[List<Guid>(users.Keys).[Random().Next(users.Count-1)]]
    member this.HasUser (name:string) = nameMap.ContainsKey(name) 
    member this.GetUser (name:string) = nameMap.TryGetValue(name) 
    member this.PutUser (user:User) = 
        users.TryAdd(user.Id,user) |> ignore
        nameMap.TryAdd(user.Name,user.Id) |> ignore
        connectionMap.TryAdd(user.Id,false) |> ignore
    member this.UpdateUser (user:User) = 
        users.[user.Id] <- user 
    // member this.GetAllUserNames () = nameMap.Keys   
    member this.GetAllUsers () = users   
    member this.GetTweet (id:Guid) = tweets.TryGetValue(id) 
    member this.PutTweet (tweet:Tweet) = 
        tweets.TryAdd(tweet.ID,tweet)  |> ignore
        tweetStore.Add(tweet)
    member this.IsConnected (id:Guid) = connectionMap.GetValueOrDefault(id,false)
    member this.MarkAsConnected (id:Guid) = connectionMap.[id] <- true
    member this.MarkAsDisConnected (id:Guid) = connectionMap.[id] <- false
    member this.AddHashTag (hashTag:string,tweetID:Guid) =
        if hashTags.ContainsKey(hashTag) then
            hashTags.[hashTag].Add(tweetID)
        else
            let newList = List<Guid> ()
            newList.Add(tweetID) 
            hashTags.TryAdd(hashTag,newList)  |> ignore
    member this.GetTweetsWithHashTag (hashTag:string) = hashTags.GetValueOrDefault(hashTag,List<Guid>())
    member this.GetAllHashTags () = hashTags.Keys
    member this.AddMention (mention:string,tweetID:Guid) =
        if mentions.ContainsKey(mention) then
            mentions.[mention].Add(tweetID)
        else
            let newList = List<Guid> ()
            newList.Add(tweetID) 
            mentions.TryAdd(mention,newList)  |> ignore
    member this.GetTweetsWithMention (mention:string) = mentions.GetValueOrDefault(mention,List<Guid>())
    member this.GetAllMentions () = mentions.Keys
    member this.QueryTweets (following:Guid[]) = 
        if following.Length > 0 then
            let ids = tweetStore.Query(following)
            Array.init ids.Length (fun i -> tweets.GetValueOrDefault((ids.[i]),null))
        else Array.empty
    member this.QueryHashTag (hashTag:string) = 
        let valid,result = hashTags.TryGetValue(hashTag)
        if not valid then Array.empty
        else Array.init result.Count (fun i -> tweets.[result.[i]])       
    member this.QueryMention (mention:string) = 
        let valid,result = mentions.TryGetValue(mention)
        if not valid then Array.empty
        else Array.init result.Count (fun i -> tweets.[result.[i]])
    member this.GetTotalNumOfMentions() = mentions.Count
    member this.GetTotalNumOfHashTags() = hashTags.Count
    member this.GetTotalNumOfTweets() = tweetStore.Size()
    member this.AddCred (userName:string,password:string) = creds.TryAdd(userName,password)  |> ignore
    member this.GetCred (userName:string) =creds.TryGetValue(userName)