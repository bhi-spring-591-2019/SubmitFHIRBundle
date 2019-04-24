using Hl7.Fhir.Model;
using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;

namespace SubmitFHIRBundle
{
    public class ReferenceConverter
    {
        public static void ConvertUUIDs(Bundle bundle)
        {
            ConvertUUIDs(bundle, CreateUUIDLookUpTable(bundle));
        }

        private static void ConvertUUIDs(Base resource, Dictionary<string, IdTypePair> idLookupTable)
        {
            try
            {
                foreach (var child in resource.Children)
                {
                    if (child.TypeName == "Reference")
                    {
                        var resourceReference = child as ResourceReference;
                        if (resourceReference != null && resourceReference.Reference != null)
                        {
                            idLookupTable.TryGetValue(resourceReference.Reference, out var idTypePair);
                            if (idTypePair != null)
                            {
                                resourceReference.Reference = idTypePair.ResourceType + "/" + idTypePair.Id;
                            }
                            else
                            {
                                Console.WriteLine("Unable to resove reference: " + resourceReference.Reference);
                            }
                        }
                    }
                    else
                    {
                        ConvertUUIDs(child, idLookupTable);
                    }
                }
            }
            catch (Exception ex)
            {
                var t = ex.Message;
                throw;
            }
        }

        private static Dictionary<string, IdTypePair> CreateUUIDLookUpTable(Bundle bundle)
        {
            Dictionary<string, IdTypePair> table = new Dictionary<string, IdTypePair>();

            try
            {
                foreach (var entry in bundle.Entry)
                {
                    table.Add(entry.FullUrl, new IdTypePair { ResourceType = entry.Resource.ResourceType, Id = entry.Resource.Id });
                }
            }
            catch
            {
                Console.WriteLine("Error parsing resources in bundle");
                throw;
            }

            return table;
        }

        private class IdTypePair
        {
            public string Id { get; set; }

            public ResourceType ResourceType { get; set; }
        }
    }
}
