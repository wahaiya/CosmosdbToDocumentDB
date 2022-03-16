using Newtonsoft.Json;

namespace CSharpSample

{  public class VoiceCount
    {
        public int TotalCount; 
    
    }
  
    public class VoiceBot
    {
        [JsonProperty(PropertyName = "id")]
        public string id { get; set; }
        public string realId { get; set; }
        public object document { get; set; }
        public string _etag { get; set; }
        public string _rid { get; set; }
        public string _self { get; set; }
        public string _attachments { get; set; }
        public string _ts { get; set; }

        public override string ToString()
        {
            return JsonConvert.SerializeObject(this);
        }

    }
}
