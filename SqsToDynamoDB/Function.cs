using Amazon.Lambda.Core;
using Amazon.Lambda.SQSEvents;
using Newtonsoft.Json;
using Amazon.SQS;
using Amazon.SQS.Model;
using System.Globalization;
using Amazon.DynamoDBv2;
using Amazon.DynamoDBv2.Model;
using Amazon.Runtime.Internal;



// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace SqsToDynamoDB;

public class Function
{

    /// <summary>
    /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
    /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
    /// region the Lambda function is executed in.
    /// </summary>
    public Function()
    {

    }


    /// <summary>
    /// This method is called for every Lambda invocation. This method takes in an SQS event object and can be used 
    /// to respond to SQS messages.
    /// </summary>
    /// <param name="evnt">The event for the Lambda function handler to process.</param>
    /// <param name="context">The ILambdaContext that provides methods for logging and describing the Lambda environment.</param>
    /// <returns></returns>
    public async Task FunctionHandler(SQSEvent evnt, ILambdaContext context)
    {
        foreach(var message in evnt.Records)
        {
            await ProcessMessageAsync(message, context);
        }
    }

    private async Task ProcessMessageAsync(SQSEvent.SQSMessage message, ILambdaContext context)
    {
        context.Logger.LogInformation($"Processed message {message.Body}");

        var jsonBatch = JsonConvert.DeserializeObject<List<PersonDTO>>(message.Body);

        bool awsRequestStatus = false;

        IAmazonDynamoDB client;

        var sqsClient = new AmazonSQSClient();

        try
        {

            foreach (var item in jsonBatch)
            {

                var dbItem = new Dictionary<string, AttributeValue>
                {
                    ["SNo"] = new AttributeValue { N = item.SNo.ToString() },
                    ["AccountNumber"] = new AttributeValue { S = item.AccountNumber },
                    ["FirstName"] = new AttributeValue { S = item.FirstName },
                    ["LastName"] = new AttributeValue { S = item.LastName },
                };
                string tableName = Environment.GetEnvironmentVariable("TABLENAME");

                using (client = new AmazonDynamoDBClient())
                {
                    var request = new PutItemRequest
                    {
                        TableName = tableName,
                        Item = dbItem,
                    };

                    //var response = await sqsClient.PutItemAsync(request);
                    //awsRequestStatus = (response.HttpStatusCode == System.Net.HttpStatusCode.OK);
                }
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex.Message);
            throw;
        }
            // insert each item from Person object to DynamoDB Table - Person
            // DynamoDB tableName should stored in Environment Variable
            //item inserted into DyanamoDB
    }


    // TODO: Do interesting work based on the new message
    //await Task.CompletedTask;

}
