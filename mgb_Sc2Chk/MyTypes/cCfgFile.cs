// Версия 1.004  от 16 декабря 2011 г.  - Классы для CFG-файлов
using	MyTypes							;

namespace MyTypes
{

	//--------------------------------------------------------
	public class CCfgFile : CDictionary
	{

		public	CCfgFile()
		{
		}

		public	CCfgFile(string FileName)
		{
			if (FileName == null)
				return;
			if (FileName.Trim().Length == 0)
				return;
			LoadFromFile(FileName);
		}

		public	CCfgFile(string FileName, int CharSet)
		{
			if (FileName == null)
				return;
			if (FileName.Trim().Length == 0)
				return;
			LoadFromFile(FileName, CharSet);
		}
/*
		public static void Main()
		{
			System.Diagnostics.Debugger.Launch();
			Err.LogToConsole();

			CCfgFile CfgFile = new CCfgFile("D:\\my.cfg");
			if (CfgFile.Contains(" Param2 ")) {
				System.Console.WriteLine("CONTAINS : {0}", CfgFile["Param2"]);
			}
			foreach ( string Str1 in CfgFile.Keys() ) {
				System.Console.WriteLine("{0}={1}", Str1, CfgFile[Str1]);
			}
			CfgFile.Remove("Param2");
			CfgFile.SaveToFile("D:\\my2.cfg");
		}
*/
	}

}