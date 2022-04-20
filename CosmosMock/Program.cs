namespace Cosmos.Samples.Shared
{
    using System;
    using System.Collections.Generic;
    using System.IO;
    using System.Linq;
    using System.Threading.Tasks;
    using Microsoft.Azure.Cosmos;
    using Microsoft.Extensions.Configuration;
    using Newtonsoft.Json;

    /// <summary>
    /// This class shows the different ways to execute item feed and queries.
    /// </summary>
    /// <remarks>
    /// Modified from https://github.com/Azure/azure-cosmos-dotnet-v3/blob/6e2609f35721fa0d16a069746ef00ed5ac5fae4e/Microsoft.Azure.Cosmos.Samples/CodeSamples/Queries/Program.cs
    /// </remarks>
    internal class Program
    {
        //Read configuration
        private static readonly string CosmosDatabaseId = "samples";
        private static readonly string containerId = "query-samples";

        private static Database cosmosDatabase = null;

        // Async main requires c# 7.1 which is set in the csproj with the LangVersion attribute
        // <Main>
        public static async Task Main(string[] args)
        {
            try
            {
                IConfigurationRoot configuration = new ConfigurationBuilder()
                    .Build();

                string connectionString = "Insert Key here";

                using (CosmosClient client = new CosmosClient(connectionString))
                {
                    await Program.RunDemoAsync(client);
                }
            }
            catch (CosmosException cre)
            {
                Console.WriteLine(cre.ToString());
            }
            catch (Exception e)
            {
                Exception baseException = e.GetBaseException();
                Console.WriteLine("Error: {0}, Message: {1}", e.Message, baseException.Message);
            }
            finally
            {
                Console.WriteLine("End of demo, press any key to exit.");
                Console.ReadKey();
            }
        }
        // </Main>

        // <RunDemoAsync>
        private static async Task RunDemoAsync(CosmosClient client)
        {
            cosmosDatabase = await client.CreateDatabaseIfNotExistsAsync(CosmosDatabaseId);
            Container everythingContainer = await Program.GetOrCreateContainerAsync(cosmosDatabase, "everything");
            Container orderContainer= await Program.GetOrCreateContainerAsync(cosmosDatabase, "order-test");
            Container commentContainer = await Program.GetOrCreateContainerAsync(cosmosDatabase, "order-comments-test");
            Container itemContainer = await Program.GetOrCreateContainerAsync(cosmosDatabase, "order-items-test");


            //       await Program.CreateItems(everythingContainer,orderContainer,commentContainer,itemContainer);
            /**
                   await Program.ItemFeed(everythingContainer, "everything");
                   await Program.ItemFeed(orderContainer, "order");
                   await Program.ItemFeed(commentContainer, "comment");
                   await Program.ItemFeed(itemContainer, "item");

                   await Program.QueryWithSqlParameters(everythingContainer, "2", "order", "everything");
                   await Program.QueryWithSqlParameters(everythingContainer, "2", "item", "everything");
                   await Program.QueryWithSqlParameters(everythingContainer, "2", "comment", "everything");

                   await Program.QueryWithSqlParameters(orderContainer, "2", "order", "order");
                   await Program.QueryWithSqlParameters(itemContainer, "2", "item", "item");
                   await Program.QueryWithSqlParameters(commentContainer, "2", "comment", "comment");
            **/
            await Comparer(everythingContainer, orderContainer, commentContainer, itemContainer);

            // Uncomment to Cleanup
            //await cosmosDatabase.DeleteAsync();
        }
        // </RunDemoAsync>

        // <ItemFeed>
        private static async Task ItemFeed(Container container, string feedType)
        {
            double totalRSUs = 0;
            // SQL
            FeedIterator<Order> setIterator = container.GetItemQueryIterator<Order>(requestOptions: new QueryRequestOptions { MaxItemCount = 100 });
            while (setIterator.HasMoreResults)
            {
                var resp = await setIterator.ReadNextAsync();
           //     Console.WriteLine($"Fetching {resp.Count} items from {feedType} took: {resp.RequestCharge} RSUS");
                totalRSUs += resp.RequestCharge;
            }

            Console.WriteLine($"Total RSUs used for {feedType} : {totalRSUs}");
        }
        // </ItemFeed>

        // <ItemStreamFeed>
        private static async Task ItemStreamFeed(Container container)
        {
            int totalCount = 0;

            // SQL
            FeedIterator setIterator = container.GetItemQueryStreamIterator();
            while (setIterator.HasMoreResults)
            {
                int count = 0;
                using (ResponseMessage response = await setIterator.ReadNextAsync())
                {
                    response.EnsureSuccessStatusCode();
                    count++;
                    using (StreamReader sr = new StreamReader(response.Content))
                    using (JsonTextReader jtr = new JsonTextReader(sr))
                    {
                        JsonSerializer jsonSerializer = new JsonSerializer();
                        dynamic array = jsonSerializer.Deserialize<dynamic>(jtr);
                        totalCount += array.Documents.Count;
                    }
                }

            }

            Assert("Expected two families", totalCount == 2);
        }
        // </ItemStreamFeed>

        // <QueryItemsInPartitionAsStreams>
        private static async Task QueryItemsInPartitionAsStreams(Container container)
        {
            // SQL
            FeedIterator setIterator = container.GetItemQueryStreamIterator(
                "SELECT F.id, F.LastName, F.IsRegistered FROM order-details F",
                requestOptions: new QueryRequestOptions()
                {
                    PartitionKey = new PartitionKey("Anderson"),
                    MaxConcurrency = 1,
                    MaxItemCount = 1
                });

            int count = 0;
            while (setIterator.HasMoreResults)
            {
                using (ResponseMessage response = await setIterator.ReadNextAsync())
                {
                    Assert("Response failed", response.IsSuccessStatusCode);
                    count++;
                    using (StreamReader sr = new StreamReader(response.Content))
                    using (JsonTextReader jtr = new JsonTextReader(sr))
                    {
                        JsonSerializer jsonSerializer = new JsonSerializer();
                        dynamic items = jsonSerializer.Deserialize<dynamic>(jtr).Documents;
                        Assert("Expected one family", items.Count == 1);
                        dynamic item = items[0];
                        Assert($"Expected LastName: Anderson Actual: {item.LastName}", string.Equals("Anderson", item.LastName.ToString(), StringComparison.InvariantCulture));
                    }
                }
            }

            Assert("Expected 1 family", count == 1);
        }
        // </QueryItemsInPartitionAsStreams>

        // <QueryWithSqlParameters>
        private static async Task<double> QueryWithSqlParameters(Container container, string orderId, string type)
        {
            // Query using two properties within each item. WHERE Id == "" AND Address.City == ""
            // notice here how we are doing an equality comparison on the string value of City

            QueryDefinition query = new QueryDefinition("SELECT * FROM container f WHERE f.orderId = @id AND f.type = @type")
                .WithParameter("@id", orderId)
                .WithParameter("@type", type);
            double totalRSUs = 0;


            FeedIterator<Item> resultSetIterator = container.GetItemQueryIterator<Item>(query, requestOptions: new QueryRequestOptions() { PartitionKey = new PartitionKey(orderId) });
            while (resultSetIterator.HasMoreResults)
            {
                var resp = await resultSetIterator.ReadNextAsync();
             //   Console.WriteLine($"Fetching {resp.Count} items via SQL from {feedType} for type {type} took: {resp.RequestCharge} RSUS");
                totalRSUs += resp.RequestCharge;
            }
            return totalRSUs;
        }


        private static async Task<double> QueryWithSqlParametersGetAll(Container container, string orderId)
        {
            // Query using two properties within each item. WHERE Id == "" AND Address.City == ""
            // notice here how we are doing an equality comparison on the string value of City

            QueryDefinition query = new QueryDefinition("SELECT * FROM container f WHERE f.orderId = @id")
                .WithParameter("@id", orderId);
            double totalRSUs = 0;


            FeedIterator<Item> resultSetIterator = container.GetItemQueryIterator<Item>(query, requestOptions: new QueryRequestOptions() { PartitionKey = new PartitionKey(orderId) });
            while (resultSetIterator.HasMoreResults)
            {
                var resp = await resultSetIterator.ReadNextAsync();
                //   Console.WriteLine($"Fetching {resp.Count} items via SQL from {feedType} for type {type} took: {resp.RequestCharge} RSUS");
                totalRSUs += resp.RequestCharge;
            }
            // Console.WriteLine($"Total RSUs used for {feedType} : {totalRSUs}");
            return totalRSUs;
        }

        private static async Task Comparer(Container everythingContainer, Container orderContainer, Container commentContainer, Container itemContainer)
        {
            double everythingAverage = 0;
            double singleAverage= 0;
            double seperateAverage= 0;
            double seperateTypeAverage = 0;

            for (int i =0; i < 1000; i++)
            {
                var everythingRSUs = await QueryWithSqlParametersGetAll(everythingContainer, $"{i}");
                var evOrders= await QueryWithSqlParameters(everythingContainer, $"{i}","order");
                var evItems = await QueryWithSqlParameters(everythingContainer, $"{i}", "item");
                var evComments = await QueryWithSqlParameters(everythingContainer, $"{i}", "comment");

                var orderRSUs = await QueryWithSqlParametersGetAll(orderContainer, $"{i}");
                var itemRSUs = await QueryWithSqlParametersGetAll(commentContainer, $"{i}");
                var commentRSUs = await QueryWithSqlParametersGetAll(itemContainer, $"{i}");

                var sOrders= await QueryWithSqlParameters(orderContainer, $"{i}", "order");
                var sItems = await QueryWithSqlParameters(itemContainer, $"{i}", "item");
                var sComments = await QueryWithSqlParameters(commentContainer, $"{i}", "comment");

                var evTotal = evOrders + evItems + evComments;
                var total = orderRSUs + itemRSUs + commentRSUs;
                var singleTotal = sOrders + sItems+ sComments;
                Console.WriteLine($"Id {i} Everything (At Once) RSUs: {everythingRSUs:0.00} Everything (Singular Calls) RSUs: {evTotal:0.00} Combined RSUs {total:0.00} Combined RSUs (with Type call) {singleTotal:0.00}");
                everythingAverage += everythingRSUs;
                singleAverage += evTotal;
                seperateAverage += total;
                seperateTypeAverage += singleTotal;
            }
            Console.WriteLine($"Everything AVG: {everythingAverage / 1000:0.00} Everything (Single Calls) AVG: {singleAverage / 1000:0.00} Combined AVG: {seperateAverage / 1000:0.00} Combined (With type) AVG: {seperateTypeAverage/ 1000:0.00}");
        }

        // </QueryWithSqlParameters>

        /// <summary>
        /// Creates the items used in this Sample
        /// </summary>
        /// <param name="container">The selfLink property for the CosmosContainer where items will be created.</param>
        /// <returns>None</returns>
        // <CreateItems>
        private static async Task CreateItems(Container everythingContainer, Container orderContainer, Container commentContainer, Container itemContainer)
        {
           for(int i = 0; i < 1000; i++)
            {
                var order = new Order
                {
                    Id=$"{i}",
                    OrderId = $"{i}",
                    SourceId = "Source" + 1,
                    Type = "Order"
                };
                await everythingContainer.UpsertItemAsync(order, new PartitionKey(order.OrderId));
                await orderContainer.UpsertItemAsync(order, new PartitionKey(order.OrderId));

                var comments = new Random().Next(0, 10);
                for(int j = 0; j < comments; j++)
                {
                    var comment = new OrderComment
                    {
                        Id = $"{i}-c",
                        OrderId = $"{i}",
                        Value = $"{j} comment",
                        Type = "Comment"
                    };
                    await everythingContainer.UpsertItemAsync(comment, new PartitionKey(comment.OrderId));
                    await commentContainer.UpsertItemAsync(comment, new PartitionKey(comment.OrderId));
                }

                var items = new Random().Next(1, 10);
                for (int j = 0; j < items; j++)
                {
                    var item = new OrderItem
                    {
                        Id = $"{i}-oi",
                        OrderId = $"{i}",
                        Value = $"{j} item",
                        Type = "Item"
                    };
                    await everythingContainer.UpsertItemAsync(item, new PartitionKey(item.OrderId));
                    await itemContainer.UpsertItemAsync(item, new PartitionKey(item.OrderId));
                }
            }
        }
        // </CreateItems>

        /// <summary>
        /// Get a DocuemntContainer by id, or create a new one if one with the id provided doesn't exist.
        /// </summary>
        /// <param name="id">The id of the CosmosContainer to search for, or create.</param>
        /// <returns>The matched, or created, CosmosContainer object</returns>
        // <GetOrCreateContainerAsync>
        private static async Task<Container> GetOrCreateContainerAsync(Database database, string containerId)
        {
            ContainerProperties containerProperties = new ContainerProperties(id: containerId, partitionKeyPath: "/orderId");

            return await database.CreateContainerIfNotExistsAsync(
                containerProperties: containerProperties,
                throughput: 400);
        }
        // </GetOrCreateContainerAsync>

        private static void Assert(string message, bool condition)
        {
            if (!condition)
            {
                throw new ApplicationException(message);
            }
        }

        public class OrderComment : Item
        {
            public string Value { get; set; }
        }
        public class OrderItem : Item
        {
            public string Value { get; set; }
        }
        public class Order: Item
        {
            public string SourceId { get; set; }
        }

        public class Item
        {
            [JsonProperty(PropertyName = "id")]
            public string Id { get; set; }
            public string Type { get; set; }
            [JsonProperty("orderId")]
            public string OrderId { get; set; }
        }
    }
}

