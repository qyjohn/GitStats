import java.io.*;
import java.util.*;
import java.math.*;
import com.amazonaws.*;
import com.amazonaws.auth.*;
import com.amazonaws.auth.profile.*;
import com.amazonaws.regions.*;
import com.amazonaws.services.sqs.*;
import com.amazonaws.services.sqs.model.*;

public class SqsWorker extends Thread
{
        public AmazonSQSClient client;
	public String sqsRegion, sqsUrl;

        public SqsWorker()
        {
		try
		{
			Properties prop = new Properties();
			InputStream input = new FileInputStream("config.properties");
			prop.load(input);
			sqsRegion = prop.getProperty("sqs_region");
			sqsUrl = prop.getProperty("sqs_url");

			client = new AmazonSQSClient();
			client.configureRegion(Regions.fromName(sqsRegion));
		}catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}
        }

	public void run()
	{
		while (true)
		{
			try
			{
				ReceiveMessageRequest request = new ReceiveMessageRequest(sqsUrl).withMaxNumberOfMessages(1).withWaitTimeSeconds(20);
				ReceiveMessageResult result = client.receiveMessage(request);
				for (Message message : result.getMessages())
				{
					String[] params = message.getBody().split(",");
					if (params.length == 3)
					{
						System.out.println("STARTING: " + params[0] + " " + params[1] + " " + params[2]);
						ProjectStats prj = new ProjectStats(params[0], params[1], params[2]);
						prj.run();
                                                System.out.println("FINISHED: " + params[0] + " " + params[1] + " " + params[2]);
					}
					client.deleteMessage(sqsUrl, message.getReceiptHandle());
				}
			} catch (Exception e)
			{
				System.out.println(e.getMessage());
				e.printStackTrace();
			}
		}
	}

        public static void main(String[] args)
        {
		try
		{
			int cores = Runtime.getRuntime().availableProcessors();
			SqsWorker workers[] = new SqsWorker[cores];
			for (int i=0; i<cores; i++)
			{
				workers[i] = new SqsWorker();
				workers[i].start();
			}
		} catch (Exception e)
		{
			System.out.println(e.getMessage());
			e.printStackTrace();
		}

        }
}

