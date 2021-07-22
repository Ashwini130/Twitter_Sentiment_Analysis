import twitter4j.*;

import static java.lang.System.exit;

public class tweetConsumer {

    public static void main(String[] args) throws TwitterException
    {
        if(args.length==0)
        {
            System.out.println("Please specify atleast one topic for sentiment analysis");
            exit(1);
        }
        TwitterStream twitterStream = new TwitterStreamFactory().getInstance();

        // Adding Listener to consume tweets
        StatusListener listener = new twitterListener();
        twitterStream.addListener(listener);

        FilterQuery filterQuery = new FilterQuery();

        //String tracks[] = new String[]{"Olympics"};
        String lang[] = new String[]{"en"};

        //filter the twitter stream with topics specified in the command line
        filterQuery.track(args);
        filterQuery.language(lang);


        // applying the filter
        twitterStream.filter(filterQuery);
    }
}