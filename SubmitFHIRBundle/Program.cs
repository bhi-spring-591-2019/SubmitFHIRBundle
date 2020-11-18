using System;
using System.IO;
using Microsoft.Extensions.CommandLineUtils;
using Hl7.Fhir.Model;
using Hl7.Fhir.Serialization;
using Newtonsoft.Json;
using Hl7.Fhir.Rest;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;

namespace SubmitFHIRBundle
{
    class Program
    {

        private static SemaphoreSlim semaphore;

        static void Main(string[] args)
        {
            var app = new CommandLineApplication();
            app.Name = "SubmitFHIRBundle";
            app.Description = "Submits all child resources in a FHIR Bundle Transaction to a specific FHIR server.";
            app.ExtendedHelpText = "Sample Usage: SubmitFHIRBunlde -p exampleBundle.json -s https://ExampleFHIRServer.com/DSTU3";
            app.HelpOption("-?|-h|--help");
            var bundlePath = app.Option("-p|--pathToBundleFile",
                                        "Full path to .json file which contains a FHIR Bundle resource that is a transaction",
                                        CommandOptionType.SingleValue);
            var bundleDir = app.Option("-d|--pathToBundleDir",
                                        "Full path to directory which contains FHIR Bundle resources that are all transactions",
                                        CommandOptionType.SingleValue);
            var serverURL = app.Option("-s|--FHIRServerURL",
                                        "FHIR Server url",
                                        CommandOptionType.SingleValue);
            var bearerToken = app.Option("-t|--token",
                                         "Authorization bearer token",
                                         CommandOptionType.SingleValue);
            var separateBundle = app.Option("-b|--separateBundle",
                                            "Flag which indicates that the tool should separate the bundle into individual resources to submit to server.",
                                            CommandOptionType.NoValue);

             app.OnExecute(() =>
            {
                if (!bundlePath.HasValue() && !bundleDir.HasValue())
                {
                    Console.WriteLine("No bundle path or directory specified.");
                    app.ShowHint();
                    return 0;
                }
                if (bundlePath.HasValue() && bundleDir.HasValue())
                {
                    Console.WriteLine("Only Path or Directory can be specified not both.");
                    app.ShowHint();
                    return 0;
                }
                if (bundlePath.HasValue() && !File.Exists(bundlePath.Value()))
                {
                    Console.WriteLine($"Unable to access bundle file: {bundlePath.Value()}");
                    return 0;
                }
                if (bundleDir.HasValue() && !Directory.Exists(bundleDir.Value()))
                {
                    Console.WriteLine($"Unable to access bundle directory: {bundleDir.Value()}");
                    return 0;
                }
                if (!serverURL.HasValue())
                {
                    Console.WriteLine("No FHIR server URL specified.");
                    app.ShowHint();
                    return 0;
                }
                Uri result;
                if (!Uri.TryCreate(serverURL.Value(), UriKind.Absolute, out result))
                {
                    Console.WriteLine($"FHIR server URL: {serverURL.Value()} does not appear to be a valid URL.  Please check it and try again.");
                    return 0;
                }

                semaphore = new SemaphoreSlim(1, 3);

                FhirClient client;

                try
                {
                    client = new FhirClient(serverURL.Value());
                    client.PreferredFormat = ResourceFormat.Json;

                    if (bearerToken != null)
                    {
                        client.OnBeforeRequest += (object sender, BeforeRequestEventArgs e) =>
                        {
                            // Replace with a valid bearer token for this server
                            e.RawRequest.Headers.Add("Authorization", "Bearer " + bearerToken);
                        };
                    }
                }
                catch (Exception ex)
                {
                    System.Console.WriteLine($"Unable to create FHIR client for server {serverURL}. Error: {ex.Message}");
                    return -1;
                }

                if (bundleDir.HasValue())
                {
                    var tasks = new List<System.Threading.Tasks.Task>();
                    var bundlePaths = Directory.EnumerateFiles(bundleDir.Value(), "*.json");
                    foreach (var path in bundlePaths)
                    {
                        tasks.Add(UploadBundle(path, client, bearerToken.Value(), separateBundle.HasValue()));
                    }

                    System.Threading.Tasks.Task.WhenAll(tasks).Wait();
                    return 0;
                }
                else
                {
                    UploadBundle(bundlePath.Value(), client, bearerToken.Value(), separateBundle.HasValue()).Wait();
                    return 0;
                }
                

            });

            app.Execute(args);
        }

        private static async Task<int> UploadBundle(string bundlePath, FhirClient client, string bearerToken = null, bool separateBundle = false)
        {
            Bundle bundle;
            try
            {
                using (StreamReader fileReader = File.OpenText(bundlePath))
                {
                    var jsonReader = new JsonTextReader(fileReader);
                    var parser = new FhirJsonParser();
                    bundle = parser.Parse<Bundle>(jsonReader);
                }
            }
            catch(Exception ex)
            {
                System.Console.WriteLine($"Unable to read/parse FHIR Bundle in file {bundlePath}. Error: {ex.Message}");
                return -1;
            }

            if (bundle.Type !=  Bundle.BundleType.Transaction && bundle.Type != Bundle.BundleType.Batch && bundle.Type != Bundle.BundleType.Collection)
            {
                System.Console.WriteLine($"This tool is designed to handle Batch, Collection or Transaction type Bundles. The supplied Bundle is of type {bundle.Type.ToString()} and connot be processed.");
                return 0;
            }

            var uploadTasks = new List<System.Threading.Tasks.Task<Resource>>();

            if (separateBundle)
            {
                try
                {
                    ReferenceConverter.ConvertUUIDs(bundle);
                }
                catch (Exception ex)
                {
                    Console.WriteLine("Failed to resolve references in doc: " + ex.Message);
                    return -1;
                }

                foreach (var entry in bundle.Entry)
                {
                    Console.WriteLine("Starting upload: " + entry.Request.Method + ":" + entry.Resource.TypeName);
                    uploadTasks.Add(UploadResourceAsync(entry.Resource, client));
                }
            }
            else
            {
                Console.WriteLine("Starting bundle upload, bundle type:" + bundle.Type.ToString());
                uploadTasks.Add(UploadResourceAsync(bundle, client));
            }
            
            while (uploadTasks.Count > 0)
            {
                var uploadTask = await System.Threading.Tasks.Task.WhenAny<Resource>(uploadTasks);
                uploadTasks.Remove(uploadTask);
                if (uploadTask.Exception != null)
                {
                    Console.WriteLine($"Error occurred uploading FHIR resource: {uploadTask.Exception.InnerException.Message}");
                }
                if (uploadTask.IsCompletedSuccessfully)
                {
                    if (uploadTask.Result != null && uploadTask.Result.TypeName != null)
                        Console.WriteLine($"Finished uploading: {uploadTask.Result.TypeName}");
                }
            }

            return 0;
        }

        private static async System.Threading.Tasks.Task<Resource> UploadResourceAsync(Resource resource, FhirClient client, bool retry = true)
        {
            await semaphore.WaitAsync();
            try
            {
                if (resource is Bundle bundle)
                    return await client.TransactionAsync(bundle);
                else
                    return await client.UpdateAsync(resource);
            }
            catch (FhirOperationException fhirEx)
            {
                if (fhirEx.Status == System.Net.HttpStatusCode.TooManyRequests && retry == true)
                {
                    // Take a break and try again
                    Console.WriteLine($"Adding a 300ms delay: {fhirEx.Message}");
                    Thread.Sleep(300);
                    return await UploadResourceAsync(resource, client, false);
                }
                else
                {
                    Console.WriteLine($"Error uploading resource to FHIR server: {fhirEx.Message}");
                    return null;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error uploading resource to FHIR server: {ex.Message}");
                return null;
            }
            finally
            {
                semaphore.Release();
            }
        }
    }
}
