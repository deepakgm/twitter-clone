#if INTERACTIVE
#load "commonUtils.fsx"
#endif

namespace ClientUtils

open System
open System.Collections.Generic
open CommonUtils

type TweetNotificationStore () =
    let maxSize=100
    let tweets = List<Tweet>(maxSize)
    let mutable size = 0 
    member this.Add(tweet:Tweet) = 
        if size < maxSize then
            tweets.Add(tweet)
            size <- size+1
        else 
            tweets.RemoveAt(0)
            tweets.Add(tweet)
    member this.GetAll() = tweets 
    member this.Print () = 
        if size = 0 then printfn "No Tweet notifcations found"
        else 
            printfn "Tweet notifcations: "
            let mutable j = 1
            for tweet in tweets do 
            printf "%i) " j
            j <- j+1
            tweet.Print()

type QueryResult () =
    let mutable id = Guid.Empty
    let mutable data:Tweet[] = Array.empty
    member this.ID = id
    member this.SetID(id1:Guid) = 
        id <- id1
        data <- Array.empty
    member this.Add(tweets:Tweet[]) = data <- Array.append data tweets
    member this.PrintSize () = 
        // printfn "**************************************"
        printfn "*** %i tweets in query results ***" data.Length
        // printfn "**************************************"
    member this.Print () = 
        printfn "*** %i tweets in query results ***" data.Length
        let mutable j = 1
        for tweet in data do
            printf "%i) " j
            j<- j+1
            tweet.Print()
        printfn "**********************************"
