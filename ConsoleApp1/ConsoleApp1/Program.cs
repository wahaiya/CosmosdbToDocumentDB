using System;
using System.Text;
using System.Linq;
using System.Collections.Generic;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Net.Security;
using MongoDB.Driver;
using MongoDB.Bson;

using System.Threading.Tasks;
using System.Configuration;
using System.Net;
using Microsoft.Azure.Cosmos;
using System.Xml.Linq;

namespace CSharpSample
{
    class Program
    {

        // The Azure Cosmos DB endpoint for running this sample.
        private static readonly string EndpointUri = ConfigurationManager.AppSettings["EndpointUri"];

        // The primary key for the Azure Cosmos account.
        private static readonly string PrimaryKey = ConfigurationManager.AppSettings["PrimaryKey"];

        // The Cosmos client instance
        private CosmosClient cosmosClient;

        // The database we will create
        private Database database;

        // The container we will create.
        private Container container;

        // The name of the database and container we will create
        private string databaseId = "VoiceBotDB-1";
        private string containerId = "bot";

       // private string databaseId = "saic-va-db-sit";
       // private string containerId = "saic-va-con-sit";

        public string template = "mongodb://{0}:{1}@{2}/test?replicaSet=rs0&readpreference={3}";
        private string username = "admin";
        private string password = "Password*******";
        private string clusterEndpoint = "ocean-documentdb-test.c22b6nswwl7n.us-west-2.docdb.amazonaws.com:27017";
        private string readPreference = "secondaryPreferred";
        private string DocumentDBconnectionString = "";

        static async Task Main(string[] args)
        {

           

            try
            {
                Console.WriteLine("Beginning operations...\n");
                Program p = new Program();
                await p.GetStartedDemoAsync();

            }
            catch (CosmosException de)
            {
                Exception baseException = de.GetBaseException();
                Console.WriteLine("{0} error occurred: {1}", de.StatusCode, de);
            }
            catch (Exception e)
            {
                Console.WriteLine("Error: {0}", e);
            }
            finally
            {
                Console.WriteLine("End of Migration, press any key to exit.");
                Console.ReadKey();
            }


        }


        private async Task GetDocumentDBAsync()
        {
            template = "mongodb://{0}:{1}@{2}/test?replicaSet=rs0&readpreference={3}";
            username = "oceanadmin";
            password = "Password01!";
            clusterEndpoint = "ocean-documentdb-test.c22b6nswwl7n.us-west-2.docdb.amazonaws.com:27017";
            readPreference = "secondaryPreferred";
            DocumentDBconnectionString = String.Format(template, username, password, clusterEndpoint, readPreference);

           // var settings = MongoClientSettings.FromUrl(new MongoUrl(DocumentDBconnectionString));
            //var client = new MongoClient(settings);

            //var database = client.GetDatabase("test");
            //var collection = database.GetCollection<BsonDocument>("samplecollection");

            //Console.WriteLine("Get DocumentDB: {0}\n", this.database.Id);
        }

        public async Task GetStartedDemoAsync()
        {
            // Create a new instance of the Cosmos Client
            this.cosmosClient = new CosmosClient(EndpointUri, PrimaryKey, new CosmosClientOptions() { ApplicationName = "CosmosDB-test-1" });
            await this.CreateDatabaseAsync();
            await this.CreateContainerAsync();
            await this.ScaleContainerAsync();

            await this.GetDocumentDBAsync();
            await this.ReplaceKeyItemsImportDocumentDB(); 



            // await this.AddItemsToContainerAsync();
            // await this.QueryItemsAsync();
            //await this.ReplaceFamilyItemAsync();
            //await this.DeleteFamilyItemAsync();
            //await this.DeleteDatabaseAndCleanupAsync();
        }
       
        private async Task CreateDatabaseAsync()
        {
            // Create a new database
            this.database = await this.cosmosClient.CreateDatabaseIfNotExistsAsync(databaseId);
            Console.WriteLine("Get CosmosDB Database: {0}\n", this.database.Id);
        }
        // </CreateDatabaseAsync>

        // <CreateContainerAsync>
        /// <summary>
        /// Create the container if it does not exist. 
        /// Specifiy "/LastName" as the partition key since we're storing family information, to ensure good distribution of requests and storage.
        /// </summary>
        /// <returns></returns>
        private async Task CreateContainerAsync()
        {
            // Create a new container
            // this.container = await this.database.CreateContainerIfNotExistsAsync(containerId, "/_rid", 400);
            this.container =this.database.GetContainer(containerId); 

            Console.WriteLine("Get CosmosDB Container: {0}\n", this.container.Id);
        }
        // </CreateContainerAsync>

        // <ScaleContainerAsync>
        /// <summary>
        /// Scale the throughput provisioned on an existing Container.
        /// You can scale the throughput (RU/s) of your container up and down to meet the needs of the workload. Learn more: https://aka.ms/cosmos-request-units
        /// </summary>
        /// <returns></returns>
        private async Task ScaleContainerAsync()
        {
            // Read the current throughput
            int? throughput = await this.container.ReadThroughputAsync();
            if (throughput.HasValue)
            {
                Console.WriteLine("Current CosmosDB provisioned throughput : {0}\n", throughput.Value);
             

                //int newThroughput = throughput.Value + 100;
                // Update throughput
                //  await this.container.ReplaceThroughputAsync(newThroughput);
                // Console.WriteLine("New provisioned throughput : {0}\n", newThroughput);
            }

        }
      

        private async Task ReplaceKeyItemsImportDocumentDB()
        {

            var settings = MongoClientSettings.FromUrl(new MongoUrl(DocumentDBconnectionString));
            var client = new MongoClient(settings);
            var database = client.GetDatabase("test");
            var collection = database.GetCollection<BsonDocument>("samplecollection");

            Console.WriteLine("Connected AWS DocumentDB: {0}\n", this.database.Id);

            var sqlQueryText1 = "SELECT count(1) as TotalCount FROM c";

            QueryDefinition queryDefinition1 = new QueryDefinition(sqlQueryText1);

            FeedIterator<VoiceCount> queryResultSetIterator1 = this.container.GetItemQueryIterator<VoiceCount>(queryDefinition1);

            int i = 0;
            List<VoiceCount> VoiceCount = new List<VoiceCount>();


            while (queryResultSetIterator1.HasMoreResults)
            {
                FeedResponse<VoiceCount> currentResultSet1 = await queryResultSetIterator1.ReadNextAsync();

       

                foreach (VoiceCount voicecount in currentResultSet1)
                {
                    i = voicecount.TotalCount; 
                    Console.WriteLine("Total CosmosDB Record Count: {0} \n", voicecount.TotalCount);
   
                }
                
            }

            Console.WriteLine("We will Process Data Migration to Document DB , Press any key to continue. \n");
            Console.ReadKey();

            var sqlQueryText = "SELECT * FROM c";

            var VoiceDocument = "";

          
            QueryDefinition queryDefinition = new QueryDefinition(sqlQueryText);

            FeedIterator<VoiceBot> queryResultSetIterator = this.container.GetItemQueryIterator<VoiceBot>(queryDefinition);

          
            List<VoiceBot> VoiceBots = new List<VoiceBot>();

            int j = 0;
           

         //   int TotoalnNUM = queryResultSetIterator.

            while (queryResultSetIterator.HasMoreResults)
            {
                FeedResponse<VoiceBot> currentResultSet = await queryResultSetIterator.ReadNextAsync();

              
                foreach (VoiceBot voicebot in currentResultSet)
                {
                    //Console.WriteLine(" realid: {0} ", voicebot.realid);
                    VoiceDocument = voicebot.document.ToString().Replace("$type", "type$").Replace("$value", "value$");
                   
                    var ss1 = "{"+ 
                                "\""  + "id"         +"\":"  + "\""  +voicebot.id +         "\"" + "," +
                                "\""  + "realid"     +"\":"  + "\""  +voicebot.realId +     "\"" + "," +
                                "\"" + "document"    +"\":"  + VoiceDocument + "," +
                        
                                "\"" + "_etag"       + "\":" + voicebot._etag+       "," +
                                "\"" + "_rid"        + "\":" + "\"" + voicebot._rid +       "\"" + "," +
                                "\"" + "_self"       + "\":" + "\"" + voicebot._self+       "\"" + "," +

                                "\"" + "_attachments"+ "\":" + "\"" + voicebot._attachments+ "\"" + "," +
                         
                                "\"" + "_ts"         + "\":" + voicebot._ts + "}"
                                ;


                   var ss = BsonDocument.Parse(ss1);
                  collection.InsertOne(ss);
                    j++; 
                 
                    if (j%2 ==0 )
                    Console.WriteLine("\t realid:{0} \t current record: {1}  \t Process {2}% \t Total Record:{3}", voicebot.realId, j, j*100/i,i);


                }
                i++; 
            }
        }



      
    }
}

