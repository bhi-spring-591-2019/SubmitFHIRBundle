using System;
using System.IO;
using Microsoft.Extensions.CommandLineUtils;
using Hl7.Fhir.Model;
using Hl7.Fhir.Serialization;
using Newtonsoft.Json;
using Hl7.Fhir.Rest;
using System.Collections.Generic;
using System.Threading;

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

                if (bundleDir.HasValue())
                {
                    int returnValue = 0;
                    var bundlePaths = Directory.EnumerateFiles(bundleDir.Value(), "*.json");
                    foreach (var path in bundlePaths)
                    {
                        returnValue = UploadBundle(path, serverURL.Value());
                    }
                    return returnValue;
                }
                else
                    return UploadBundle(bundlePath.Value(), serverURL.Value());

            });

            app.Execute(args);
        }

        private static int UploadBundle(string bundlePath, string serverURL)
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

            if (bundle.Type !=  Bundle.BundleType.Transaction && bundle.Type != Bundle.BundleType.Batch)
            {
                System.Console.WriteLine($"This tool is designed to handle Batch or Transaction type Bundles. The supplied Bundle is of type {bundle.Type.ToString()} and connot be processed.");
                return 0;
            }

            FhirClient client;

            try
            {
                client = new FhirClient(serverURL);
            }
            catch(Exception ex)
            {
                System.Console.WriteLine($"Unable to create FHIR client for server {serverURL}. Error: {ex.Message}");
                return -1;
            }

            try
            {
                ReferenceConverter.ConvertUUIDs(bundle);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed to resolve references in doc: " + ex.Message);
                return -1;
            }

            var uploadTasks = new List<System.Threading.Tasks.Task<Resource>>();
            semaphore = new SemaphoreSlim(10, 10);

            foreach (var entry in bundle.Entry)
            {
                Console.WriteLine("Starting upload: " + entry.Request.Method + entry.Resource.TypeName);
                uploadTasks.Add(UploadResourceAsync(entry.Resource, client));
            }
            
            while (uploadTasks.Count > 0)
            {
                var uploadTask = System.Threading.Tasks.Task.WhenAny<Resource>(uploadTasks).Result;
                uploadTasks.Remove(uploadTask);
                if (uploadTask.Exception != null)
                {
                    Console.WriteLine($"Error occurred uploading FHIR resource: {uploadTask.Exception.InnerException.Message}");
                }
                if (uploadTask.IsCompletedSuccessfully)
                {
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
                return await client.UpdateAsync(resource);
            }
            catch (FhirOperationException fhirEx)
            {
                if (fhirEx.Status == System.Net.HttpStatusCode.TooManyRequests && retry == true)
                {
                    // Take a break and try again
                    Thread.Sleep(3000);
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
