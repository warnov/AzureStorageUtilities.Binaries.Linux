using AzureStorageUtilities.PageToBlockMover.Common;

using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage.Queue;


using System;
using System.Collections.Generic;
using System.Linq;
using System.Diagnostics;
using System.IO;
using System.Text;
using System.Threading;
using System.ComponentModel;
using System.Net;
using System.Text.RegularExpressions;

namespace AzureStorageUtilities.PageToBlockMover.BatchWorker
{
    class BatchWorker
    {

        #region Fields
        static string[] arguments;
        static MovementConfiguration conf;
        static CloudStorageAccount srcStorageAccount, destStorageAccount;
        static string srcSAS, destSAS;
        static string workerId;
        static CloudTableClient tableClient;
        static CloudBlobClient srcBlobClient, destBlobClient;
        static CloudQueueClient queueClient;
        static CloudQueue jobsQueue;
        static CloudQueueMessage jobMessage;
        static CloudTable jobsTable;
        static CloudBlobContainer srcContainer, destContainer;
        static CloudBlob currentBlob;
        static DynamicTableEntity jobInfo;
        static string jobId;
        static string currentBlobUrl;
        static Process cmd;
        static string dataPath;
        const int N_ARGS = 3;
        static string version;

        #endregion

        static void Main(string[] args)
        {
            string option = CheckArguments(args);
            arguments = new string[N_ARGS];
            //Initialize static environment configurator
            EnvironmentConfigurator.SetConfiguration();

            switch (option)
            {
                case "version":
                    var assembly = System.Reflection.Assembly.GetExecutingAssembly();
                    version = FileVersionInfo.GetVersionInfo(assembly.Location).FileVersion;

                    Console.WriteLine($"AzureStorageUtilities.PageToBlockMover.BatchWorker V{version}");
                    break;
                case "online":
                    if (ReadOnlineArguments(args[1]))
                        MainExecution();
                    else
                        LogUpdate("Unable to read configuration from the cloud. Terminating.");
                    break;
                default://direct
                    SetArguments(args);
                    MainExecution();
                    break;
            }
        }

        private static void SetArguments(string[] args)
        {
            Array.Copy(args, arguments, args.Length);
        }

        /// <summary>
        /// Read the equivalent to the command line arguments, from a text files
        /// stored online; maybe a blob on Azure accessible with a SAS. This SAS is passed as an argument to this program to run unattended. So the SAS wil remain the same, but the content of the file online could be changed and get the program to execute a different job each time.
        /// </summary>
        private static bool ReadOnlineArguments(string onlineArgumentsPath)
        {
            try
            {
                var wc = new WebClient();
                var configurationText = wc.DownloadString(onlineArgumentsPath);
                var parameters = Regex.Matches(configurationText, "\"[^\"]*\"|[^ ]+");
                for (int i = 0; i < N_ARGS; i++)
                {
                    arguments[i] = parameters[i].Value.Replace("\"", "").Trim();
                }
                return true;
            }
            catch (Exception exc)
            {
                LogUpdate($"Error reading batch from the cloud: {exc.Message}");
                return false;
            }
        }

        static void MainExecution()
        {           

            StartLifeStateDaemon();
            IdentifyWorker();
            if (SetEnvironment(arguments))
                Execute();
        }

        private static string CheckArguments(string[] args)
        {
            if (args[0].Contains("--"))
            {
                return args[0].Substring(2);
            }
            else
                return "direct";

        }

        private static void IdentifyWorker()
        {

            string cmdText = EnvironmentConfigurator.WorkerIdShellCommand;

            if (!EnvironmentConfigurator.OnWindows)
            {
                if (!Directory.Exists("/sys/class/dmi/id/"))
                {
                    //We are in Linux Subsystem inside Windows, but the product uuid is not present               
                    cmdText = $"echo WindowsSubsystem{DateTime.Now.ToShortDateString()}";
                }
            }
            var response = ShellExecute(cmdText);

            if (EnvironmentConfigurator.OnWindows)
                response = response.Split("\n")[5].Trim();

            workerId = $"{System.Environment.MachineName}-{response}";
        }

        private static string ShellExecute(string cmdText)
        {
            PrepareShell();

            cmd.StandardInput.WriteLine(cmdText);
            cmd.StandardInput.Flush();
            cmd.StandardInput.Close();
            return cmd.StandardOutput.ReadToEnd();
        }

        private static void PrepareShell()
        {
            cmd = new Process();
            cmd.StartInfo.FileName = EnvironmentConfigurator.ShellPath;
            cmd.StartInfo.RedirectStandardInput = true;
            cmd.StartInfo.RedirectStandardOutput = true;
            cmd.StartInfo.CreateNoWindow = false;
            cmd.StartInfo.UseShellExecute = false;
            cmd.Start();
        }

        private static bool SetEnvironment(string[] arguments)
        {
            //Read src storage account
            conf.SrcAccountConnectionString = arguments[0];
            conf.CustomerId = arguments[1];
            conf.BatchId = arguments[2];

            try
            {
                //Creates Logs directory if it doesn't exist           
                Directory.CreateDirectory(EnvironmentConfigurator.LogsFolder);
            }
            catch
            {
                Console.WriteLine("Couldn't create logs folder");
                return false;
            }

            //Starting program information
            Presentation();
            LogUpdate($"\nBATCH:\t{conf.BatchId}\nCUST:\t{conf.CustomerId}\n" +
                $"WKRID\t{workerId}");
            InsertConsoleSeparator(3, true);



            //Acquiring storage account objects
            try
            {
                srcStorageAccount = CloudStorageAccount.Parse(conf.SrcAccountConnectionString);
            }
            catch
            {
                LogUpdate("Invalid src connection string. Terminating.");
                return false;
            }

            //Getting the SAS
            srcSAS = GetAccountSASToken(srcStorageAccount, true);
            LogUpdate("Source storage accounts validated and SAS generated OK\n\n");

            //Configure storage clients
            tableClient = srcStorageAccount.CreateCloudTableClient();
            srcBlobClient = srcStorageAccount.CreateCloudBlobClient();
            queueClient = srcStorageAccount.CreateCloudQueueClient();


            //Setting up queue and table to be used
            jobsQueue = queueClient.GetQueueReference($"{EnvironmentConfigurator.JobsQueuePrefix}-{conf.BatchId}");
            jobsTable = tableClient.GetTableReference(EnvironmentConfigurator.ProgressTable);

            //Read parameters and setup conf object
            try
            {
                ReadParameters();
            }
            catch
            {
                LogUpdate("Problem reading parameters. Terminating");
                return false;
            }

            //Setup destiny storage account
            try
            {
                destStorageAccount = CloudStorageAccount.Parse(conf.DestAccountConnectionString);
                destBlobClient = destStorageAccount.CreateCloudBlobClient();
                destSAS = GetAccountSASToken(destStorageAccount, false);
                destContainer = destBlobClient.GetContainerReference(conf.DestContainerName);
                if (destContainer.CreateIfNotExistsAsync().Result)
                {
                    LogUpdate($"{conf.DestContainerName} created.");
                }
            }
            catch
            {
                LogUpdate("Invalid dest connection string. Termintating.");
                return false;
            }

            return ValidateDataPath();
        }

        private static void InsertConsoleSeparator(int blankLines = 0, bool big = false)
        {
            if (big)
                Console.WriteLine("======================================================");
            else Console.WriteLine("==========================");
            for (int i = 0; i < blankLines; i++)
            {
                Console.WriteLine();
            }
        }

        private static void Presentation()
        {
            Console.BackgroundColor = ConsoleColor.Green;
            Console.ForegroundColor = ConsoleColor.Black;
            Console.WriteLine($"\n\n\n\n\n============================================================\n" +
                $"AzureStorageUtilities.PageToBlockMover.BatchWorker v{version}\n" +
                $"============================================================\n\n");
            Console.BackgroundColor = ConsoleColor.Black;
            Console.ForegroundColor = ConsoleColor.Green;
            Console.WriteLine("By: @warnov\nOpen Source: http://warnov.com/@page2block \n\n\n");
        }

        public static bool ValidateDataPath()
        {
            try
            {
                dataPath = EnvironmentConfigurator.DataFolder(conf.LocalTempPath);
                Directory.CreateDirectory(dataPath);
                return true;
            }
            catch (Exception exc)
            {
                LogUpdate("Invalid local path. Program terminating. " + exc.Message);
                return false;
            }
        }

        private static void LogUpdate(string message, bool withTime = true)
        {
            if (withTime) message = $"{Utilities.Now2Log}:\t{message}";
            File.AppendAllText($"{EnvironmentConfigurator.LogsFolder}{Path.DirectorySeparatorChar}{conf.BatchId}.log", $"{message}\n");
            Console.WriteLine(message);
        }



        static string GetAccountSASToken(CloudStorageAccount storageAccount, bool source)
        {
            var permissions = source ? SharedAccessAccountPermissions.Read |
                    SharedAccessAccountPermissions.List | SharedAccessAccountPermissions.Delete :
                    SharedAccessAccountPermissions.Create |
                    SharedAccessAccountPermissions.Write;

            // Create a new access policy for the account.
            SharedAccessAccountPolicy policy = new SharedAccessAccountPolicy()
            {
                Permissions = permissions,
                Services = SharedAccessAccountServices.Blob,
                ResourceTypes = SharedAccessAccountResourceTypes.Container |
                    SharedAccessAccountResourceTypes.Object,
                SharedAccessExpiryTime = DateTime.UtcNow.AddHours(96),
                Protocols = SharedAccessProtocol.HttpsOnly
            };
            // Return the SAS token.
            return storageAccount.GetSharedAccessSignature(policy);
        }

        private static void ReadParameters()
        {
            //get conf from table
            var confTable = tableClient.GetTableReference(EnvironmentConfigurator.ParamsTable);

            //assembling query
            var customerFilter = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, conf.CustomerId);
            var batchFilter = TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, conf.BatchId);
            var combinedFilters = TableQuery.CombineFilters(customerFilter, TableOperators.And, batchFilter);
            var query = new TableQuery().Where(combinedFilters);
            var conToken = new TableContinuationToken();


            //executing query
            IEnumerable<DynamicTableEntity> virtualResults =
                confTable.ExecuteQuerySegmentedAsync(query, conToken).Result.ToList();
            var confRecord = virtualResults.ToList().FirstOrDefault();

            //setting conf with query results
            conf.LocalTempPath = confRecord.Properties["LocalTempPath"].StringValue;
            conf.SafeDeleteFromSource = (bool)confRecord.Properties["SafeDeleteFromSource"].BooleanValue;
            conf.SrcPattern = confRecord.Properties["SrcBlobName"].StringValue;
            conf.SrcContainerName = confRecord.Properties["SrcContainerName"].StringValue;
            conf.AzCopyPath = confRecord.Properties["AzCopyPath"].StringValue;
            conf.CustomerId = confRecord.Properties["CustomerId"].StringValue;
            conf.DeleteFromLocalTemp = (bool)confRecord.Properties["DeleteFromLocalTemp"].BooleanValue;
            conf.DeleteFromSource = (bool)confRecord.Properties["DeleteFromSource"].BooleanValue;
            conf.DestAccountConnectionString = confRecord.Properties["DestAccountConnectionString"].StringValue;
            conf.DestContainerName = confRecord.Properties["DestContainerName"].StringValue;
            conf.DestTier = confRecord.Properties["DestTier"].StringValue;
            conf.OverwriteIfExists = (bool)confRecord.Properties["OverwriteIfExists"].BooleanValue;
        }

        private static void Execute()
        {
            while (true)
            {
                if (GetJobFromQueue())
                {
                    if (InitializeJob())
                        ExecuteJob();
                    else LogUpdate($"Job {jobId} skipped. Fetching new job");
                }
                else
                {
                    LogUpdate($"No more messages found on queue. Waiting {EnvironmentConfigurator.QueueWaitMinutes} minutes before trying again");
                    Thread.Sleep(TimeSpan.FromMinutes(EnvironmentConfigurator.QueueWaitMinutes));
                }
            }
        }

        private static bool GetJobFromQueue()
        {
            //getting the job info from queue
            jobMessage = jobsQueue.GetMessageAsync(
                TimeSpan.FromMinutes(EnvironmentConfigurator.MaxMinutesPerDownload),
                null, null).Result;

            bool ret;
            if (ret = jobMessage != null)
            {
                jobId = jobMessage.Id;
                currentBlobUrl = jobMessage.AsString;
            }
            return ret;
        }

        /// <summary>
        /// Generates a record in the jobstable and initializes the currentBlob
        /// </summary>
        private static bool InitializeJob()
        {

            //Initialize Job
            jobInfo = new DynamicTableEntity()
            {
                PartitionKey = conf.BatchId,
                RowKey = jobId
            };
            jobInfo.Properties.Add("WorkerId", EntityProperty.GeneratePropertyForString(workerId));
            jobInfo.Properties.Add("BlobUrl", EntityProperty.GeneratePropertyForString(currentBlobUrl));
            jobInfo.Properties.Add("Status", EntityProperty.GeneratePropertyForString("Started"));
            jobInfo.Properties.Add("DownStarted", EntityProperty.GeneratePropertyForString(Utilities.Now2Log));



            //preparing entity for inserting the job info in the jobs table
            var blobName = Utilities.BlobNameByUrl(currentBlobUrl);
            srcContainer = srcBlobClient.GetContainerReference(conf.SrcContainerName);
            bool sourceExists;
            if (sourceExists = srcContainer.ExistsAsync().Result) //Checking existence of source blob
            {
                var blobReference = srcContainer.GetBlobReference(blobName);
                if (sourceExists = blobReference.ExistsAsync().Result)
                {
                    //checking existence of destiny blog. If it exists, the line parameter checking overwrites is evaluated
                    var destBlobReference = destContainer.GetBlobReference(blobName);
                    var destinyExists = destBlobReference.ExistsAsync().Result;
                    var okToUpload = !destinyExists || conf.OverwriteIfExists;
                    if (okToUpload)
                    {
                        currentBlob = (CloudBlob)srcBlobClient.GetBlobReferenceFromServerAsync(new Uri(currentBlobUrl)).Result;
                        _ = currentBlob.FetchAttributesAsync();
                        jobInfo.Properties.Add("Size", EntityProperty.GeneratePropertyForLong(currentBlob.Properties.Length));
                        UpdateJobInfo();
                        return true;
                    }
                    else
                    {
                        UpdateJobInfo("DestBlobExists");
                        LogUpdate($"Destiny {blobName} blob already exists and overwriting has not been declared in the batch parameter. Job will be removed from queue.");
                    }
                }
            }
            if (!sourceExists)
            {
                UpdateJobInfo("SrcBlobMissing");
                LogUpdate($"Source {blobName} blob missing. Job will be removed from queue.");
            }
            jobsQueue.DeleteMessageAsync(jobMessage);
            return false;
        }

        private static void UpdateJobInfo(string status = "")
        {
            if (!String.IsNullOrEmpty(status))
            {
                jobInfo.Properties["Status"] = EntityProperty.GeneratePropertyForString(status);
            }
            TableOperation operation =
                TableOperation.InsertOrMerge(jobInfo);
            jobsTable.ExecuteAsync(operation);
        }


        private static void ExecuteJob()
        {
            //Set up
            bool fail = false;
            InsertConsoleSeparator();
            var size = Utilities.GetBytesReadable(currentBlob.Properties.Length);
            var localPath = $"{Path.Combine(dataPath, currentBlob.Name)}";
            var downloadCommand = $"{conf.AzCopyPath} cp \"{currentBlob.Uri}{srcSAS}\" \"{localPath}\"";
            var destUrl = $"{destStorageAccount.BlobEndpoint.AbsoluteUri}{conf.DestContainerName}/{currentBlob.Name}";
            var uploadCommand = $"{conf.AzCopyPath} cp \"{localPath}\" \"{destUrl}{destSAS}\" --block-blob-tier {conf.DestTier}";

            //Job's summary
            LogUpdate($"\nJOB:\t{jobInfo.RowKey}\nBATCH:\t{jobInfo.PartitionKey}\nFILE:\t{currentBlob.Name}\n" +
                $"URL:\t{currentBlob.Uri.AbsoluteUri}\nSIZE:\t{size}\nREMJOBS:\t{jobsQueue.ApproximateMessageCount}\n\n" +
                $"*azcopy download command: {downloadCommand}\n" +
                $"*azcopy upload command: {uploadCommand}\n\n\nDownloading...");

            //download 
            UpdateJobInfo("Downloading");
            var output = ShellExecute(downloadCommand);
            LogUpdate($"Result from azcopy download:\n{CleanOutput(output)}");
            if (AzCopyExecutedOK(output)) //No error. Continuing uploading
            {
                //upload                    
                jobInfo.Properties.Add("UpStarted", EntityProperty.GeneratePropertyForString(Utilities.Now2Log));
                LogUpdate($"Download finished ok. Now uploading...");
                UpdateJobInfo("Uploading");
                output = ShellExecute(uploadCommand);
                LogUpdate($"Result from azcopy upload:\n{CleanOutput(output)}");
                if (AzCopyExecutedOK(output)) //No errors, continue deleting
                {
                    //delete
                    if (conf.DeleteFromSource)
                    {
                        bool safe2Delete = !conf.SafeDeleteFromSource;
                        if (conf.SafeDeleteFromSource)
                        {
                            if (!(safe2Delete = destContainer.GetBlobReference(currentBlob.Name).ExistsAsync().Result))
                            {
                                LogUpdate($"{currentBlob.Uri} will not be deleted from source, because it is not yet in the destination");
                                fail = true;
                            }
                        }
                        if (safe2Delete)
                        {
                            jobInfo.Properties.Add("SrcDeleting", EntityProperty.GeneratePropertyForString(Utilities.Now2Log));
                            UpdateJobInfo("DeletingSrc");
                            LogUpdate($"{currentBlob.Uri} will be deleted from source");
                            _ = currentBlob.DeleteAsync();

                        }
                    }
                    if (conf.DeleteFromLocalTemp)
                    {
                        jobInfo.Properties.Add("LocalDeleting", EntityProperty.GeneratePropertyForString(Utilities.Now2Log));
                        UpdateJobInfo("DeletingTmp");
                        File.Delete(localPath);
                        LogUpdate($"{localPath} has been deleted");
                    }
                }
                else
                {
                    fail = true;
                    LogUpdate($"Unable to upload.\n{uploadCommand}\nWas not successful");
                }
            }
            else
            {
                fail = true;
                LogUpdate($"Unable to download.\n{downloadCommand}\nWas not successful");
            }
            //Message processed. Let's erase it (even when errors, because if there are errors it will be re-enqueued)
            jobsQueue.DeleteMessageAsync(jobMessage);
            if (!fail)
            {
                UpdateJobInfo("Completed");
                LogUpdate($"Job {jobId} finished\n\n");
            }
            else
            {
                UpdateJobInfo("Faulted");
                jobsQueue.AddMessageAsync(jobMessage);
                LogUpdate($"Job {jobId} has been faulted and requeued.\n\n");
            }
        }

        private static bool AzCopyExecutedOK(string output)
        {
            return output.Contains("Completed: 1");
        }

        private static string CleanOutput(string output)
        {
            if (EnvironmentConfigurator.OnWindows)
            {
                var lines = output.Split('\n');
                var cleanOutput = new StringBuilder();
                for (int i = 4; i < lines.Length - 3; i++)
                {
                    cleanOutput.AppendLine(lines[i]);
                }
                return cleanOutput.ToString();
            }
            else return output;
        }

        private static void StartLifeStateDaemon()
        {
            BackgroundWorker bg = new BackgroundWorker();
            bg.DoWork += ReportAlive;
            bg.RunWorkerAsync();
        }

        private static void ReportAlive(object sender, DoWorkEventArgs e)
        {
            while (true)
            {
                if (jobInfo != null)
                {
                    jobInfo.Properties["LastSeen"] = EntityProperty.GeneratePropertyForString(DateTime.UtcNow.ToOffsetShortDateTimeString(EnvironmentConfigurator.HoursOffset));
                    TableOperation operation =
                        TableOperation.InsertOrMerge(jobInfo);
                    jobsTable.ExecuteAsync(operation);
                }
                Thread.Sleep(30000);
            }
        }
    }
}
