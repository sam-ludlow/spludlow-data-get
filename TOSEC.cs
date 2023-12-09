using System.IO.Compression;
using HtmlAgilityPack;

namespace spludlow_data_get
{
	
	public class TOSEC
	{
		private readonly Dictionary<string, string> parameters;
		private readonly HttpClient httpClient;
		private readonly string directory;

		public TOSEC(Dictionary<string, string> _parameters, HttpClient _httpClient)
		{
			parameters = _parameters;
			httpClient = _httpClient;

			directory = Path.Join(parameters["DIRECTORY"], "TOSEC");

			Directory.CreateDirectory(directory);
		}


		//	latest remote
		//	latest local

		public async Task<int> Get()
		{
			Tools.RequiredParamters(parameters, "GET", ["DIRECTORY", "ACTION"]);

			string baseUrl = "https://www.tosecdev.org/downloads";

			string html = await httpClient.GetStringAsync(baseUrl);

			HtmlDocument doc = new();
			doc.LoadHtml(html);

			SortedDictionary<string, string> versionLinks = [];

			foreach (HtmlNode node in doc.DocumentNode.SelectNodes("//div[@class='pd-subcategory']"))
			{
				HtmlNode linkNode = node.ChildNodes.Single();

				string text = linkNode.InnerText;

				if (text.Length != 10)
					continue;

				string href = linkNode.Attributes["href"].Value;

				href = new Uri(new Uri(baseUrl), href).AbsoluteUri;

				versionLinks.Add(text, href);
			}

			string version = versionLinks.Keys.Last();
			baseUrl = versionLinks[version];

			string versionDirectory = Path.Combine(directory, version);

			if (Directory.Exists(versionDirectory) == true)
				return 0;

			Directory.CreateDirectory(versionDirectory);

			html = await httpClient.GetStringAsync(baseUrl);
			doc = new();
			doc.LoadHtml(html);

			string? foundUrl = null;

			foreach (HtmlNode node in doc.DocumentNode.SelectNodes("//a"))
			{
				string? href = node.Attributes["href"]?.Value;

				if (href == null)
					continue;

				if (node.InnerText.ToLower().EndsWith(".zip") == false)
					continue;

				if (foundUrl == null)
					foundUrl = new Uri(new Uri(baseUrl), href).AbsoluteUri;
				else
					throw new ApplicationException("Found more than one ZIP link.");
			}

			if (foundUrl == null)
				throw new ApplicationException("Did not find ZIP link.");

			string archiveFilename = Path.Combine(versionDirectory, "TOSEC.zip");

			Console.Write($"NEW downloading {version} {foundUrl} => {archiveFilename} ...");

			using (Stream sourceStream = await httpClient.GetStreamAsync(foundUrl))
			{
				using FileStream targetSStream = new(archiveFilename, FileMode.Create);
				sourceStream.CopyTo(targetSStream);
			}

			Console.WriteLine("...done");

			Console.Write($"extracting archive {archiveFilename} ...");
			
			ZipFile.ExtractToDirectory(archiveFilename, versionDirectory);
			
			Console.WriteLine("...done");

			return 1;
		}


		//public async Task<int> Xml()
		//{
		//	Tools.RequiredParamters(parameters, "XML", ["DIRECTORY", "ACTION"]);


		//	//Console.WriteLine("Hello " + userAgent);

		//	return 0;
		//}

	}
}
