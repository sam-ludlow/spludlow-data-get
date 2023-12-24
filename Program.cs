using System.Reflection;

string namespaceName = "spludlow_data_get";

Version? version = Assembly.GetExecutingAssembly().GetName().Version;
string assemblyVersion = version != null ? $"{version.Major}.{version.Minor}" : "0.0";

string userAgent = $"spludlow-data-get/{assemblyVersion} (https://github.com/sam-ludlow/spludlow-data-get)";

DateTime startTime = DateTime.Now;

Console.WriteLine();
Console.WriteLine($"Starting at: {startTime}");
Console.WriteLine(userAgent);
Console.WriteLine();

int exitCode = 0;

try
{
	HttpClient httpClient = new ();
	httpClient.DefaultRequestHeaders.Add("User-Agent", userAgent);

	Dictionary<string, string> parameters = [];

	foreach (string arg in args)
	{
		int index = arg.IndexOf('=');
		if (index == -1)
			throw new ApplicationException($"Bad argument format expecting KEY=VALUE : '{arg}'");

		parameters.Add(arg[..index].ToUpper(), arg[(index + 1)..]);
	}

	if (parameters.ContainsKey("DIRECTORY") == false)
		parameters.Add("DIRECTORY", Environment.CurrentDirectory);

	if (parameters.ContainsKey("VERSION") == false)
		parameters.Add("VERSION", "0");

	if (parameters.ContainsKey("DATA") == false && parameters.ContainsKey("ACTION") == false)
		throw new ApplicationException("Usage........");

	string data = parameters["DATA"].ToUpper();

	string action = parameters["ACTION"];
	action = char.ToUpper(action[0]) + action[1..].ToLower();

	Type type = Type.GetType($"{namespaceName}.{data}") ?? throw new ApplicationException($"Data Type not found: \"{data}\".");

	object instance = Activator.CreateInstance(type, new object[] { parameters, httpClient }) ?? throw new ApplicationException($"CreateInstance returned null \"{data}\".");

	MethodInfo method = type.GetMethod(action) ?? throw new ApplicationException($"Data Action not found \"{data}\", \"{action}\".");

	object result = method.Invoke(instance, null) ?? throw new ApplicationException($"Data Action did not return anything.");

	if (result is not Task<int>)
		throw new ApplicationException($"Bad return type from method \"{result.GetType().Name}\".");

	exitCode = await (Task<int>)result;

}
catch (Exception e)
{
	if (e is TargetInvocationException && e.InnerException != null)
		e = e.InnerException;

	Console.WriteLine(e.ToString());

	exitCode = -1;
}

DateTime endTime = DateTime.Now;
TimeSpan timeTook = endTime - startTime;

Console.WriteLine();
Console.WriteLine($"Finished at: {endTime}, Seconds taken: {Math.Round(timeTook.TotalSeconds, 1)}");
Console.WriteLine();

Environment.Exit(exitCode);
