using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Model;
using CsvHelper;
using CsvHelper.Configuration;
using System.Globalization;
using Newtonsoft.Json;
using Amazon.SQS;
using Amazon.SQS.Model;
using Amazon.Runtime.Internal.Endpoints.StandardLibrary;
using Amazon.Lambda.SQSEvents;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace S3ToSQSProcessor;

public class Function
{
    //private static CultureInfo configuration;
    IAmazonS3 S3Client { get; set; }

    public Function()
    {
        S3Client = new AmazonS3Client();
    }


    public Function(IAmazonS3 s3Client)
    {
        this.S3Client = s3Client;
    }

    public async Task FunctionHandler(S3Event evnt, ILambdaContext context)
    {
        var listPersons = new List<PersonDTO>();
        try
        {
            var configuration = new CsvConfiguration(CultureInfo.InvariantCulture)
            {

                HasHeaderRecord = true,
                Delimiter = ",",
                Comment = '%'
            };
            var eventRec = evnt.Records ?? new List<S3Event.S3EventNotificationRecord>();

            foreach (var record in eventRec)
            {
                var S3Event = record.S3;
                if (S3Event != null)
                {
                    var bucket = S3Event.Bucket.Name;
                    var key = S3Event.Object.Key;
                    
                    //int batchsize = 10;
                    int batchsize = Convert.ToInt32(Environment.GetEnvironmentVariable("BATCHSIZE"));

                    string fileExtension = Path.GetExtension(key); //get extension

                    if (fileExtension.Equals(".csv", StringComparison.OrdinalIgnoreCase)) //if file is .csv
                    {
                        using (GetObjectResponse response = await S3Client.GetObjectAsync(bucket, key))
                        using (Stream responseStream = response.ResponseStream)
                        using (StreamReader streamReader = new StreamReader(responseStream))
                        using (CsvReader csvReader = new CsvReader(streamReader, configuration))
                        {
                            csvReader.Context.RegisterClassMap<PersonMap>();
                            var records = csvReader.GetRecords<PersonDTO>();
                            listPersons = records.ToList();
                            Console.WriteLine(listPersons.Count + " Persons in a List");
                            SplitCsvFile(listPersons, batchsize); //Calling the Split function 
                        }
                    }
                    else
                    {
                        Console.WriteLine("The file is not a csv file");
                    }
                }
            }
        }
        catch (Exception e)
        {
            throw;
        }
    }

    public async void SplitCsvFile(List<PersonDTO> listPersons, int batchSize)
    {
        var splitList = new List<List<PersonDTO>>(); 
        try
        {
            for (int i = 0; i < listPersons.Count; i += batchSize)
            {        
                //GetRange value for minimum listperson batch
                splitList.Add(listPersons.GetRange(i, Math.Min(batchSize, listPersons.Count - i)));
            }
            foreach (var batch in splitList)
            {
                Console.WriteLine(batch.Count);
                //Console.WriteLine(batch);
                //string Json = JsonConvert.SerializeObject(splitList, Formatting.Indented);
                string Json = JsonConvert.SerializeObject(batch);
                
                Console.WriteLine(Json);
                //Console.WriteLine(splitList);

                var sqsClient = new AmazonSQSClient();

                //string qUrl = Environment.GetEnvironmentVariable("SQSURL");
                string qUrl = "https://sqs.ap-south-1.amazonaws.com/125701582174/MessageReadingQueue";
                SendMessageResponse response = await sqsClient.SendMessageAsync(qUrl, Json);
                Console.WriteLine("Message ID : " + response.MessageId);
            }
        }
        catch(Exception e)
        {
            Console.WriteLine("Error : " + e.Message);

            throw;
        }


    }

}





