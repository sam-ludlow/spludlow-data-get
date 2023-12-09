
namespace spludlow_data_get
{
	public class Tools
	{
		public static void RequiredParamters(Dictionary<string, string> parameters, string action, string[] requiredNames)
		{
			foreach (string name in requiredNames)
				if (parameters.ContainsKey(name) == false)
					throw new ApplicationException($"The action \"{action}\" requires the paramters \"{string.Join(", ", requiredNames)}\".");
		}
	}


}
