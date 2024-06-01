using Amazon.Lambda.Core;
using Amazon.SQS;
using Amazon.SQS.Model;


// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace DemoSqsMessage;

public class Function
{

    /// <summary>
    /// A simple function that takes a string and does a ToUpper
    /// </summary>
    /// <param name="input">The event for the Lambda function handler to process.</param>
    /// <param name="context">The ILambdaContext that provides methods for logging and describing the Lambda environment.</param>
    /// <returns></returns>
    //public string qUrl = "https://sqs.ap-south-1.amazonaws.com/125701582174/MyFirstQueue-24";


    public async Task<string> FunctionHandler(string input, ILambdaContext context)
    {
        var sqsClient = new AmazonSQSClient();
        string qUrl = Environment.GetEnvironmentVariable("SQSURL");

        //IAmazonSQS sqsClient;
        SendMessageResponse response = await sqsClient.SendMessageAsync(qUrl, input);     

        return response.MessageId;
      
    }
}
