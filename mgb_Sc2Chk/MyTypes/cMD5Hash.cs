// Версия 1.003  от 6 марта 2013 г.  - CMd5Hash  -  Контрольная сумма  MD5

namespace MyTypes
{
	public	class	CMd5Hash
	{
		byte[]					Data		;
		System.StringComparer			Comparer	;
		System.Text.StringBuilder		Md5Hash		;
		System.Security.Cryptography.MD5	Md5Hasher	;

		public	CMd5Hash()
		{
			Md5Hash		= new	System.Text.StringBuilder();
			Comparer	=	System.StringComparer.OrdinalIgnoreCase;
			Md5Hasher	=	System.Security.Cryptography.MD5.Create();
		}

		public string GetHash(string InputStr)
		{
			Md5Hash.Length	=	0;
			if	( InputStr == null )
				return "";
			if	( InputStr == "" )
				return "";
			Data	=	Md5Hasher.ComputeHash(System.Text.Encoding.Default.GetBytes(InputStr));
			int I;
			for ( I = 0; I <= (Data.Length - 1) ; I++)
			{
				Md5Hash.Append( Data[I].ToString("x2") );
			}
			return	Md5Hash.ToString().ToUpper();
		}

		public bool IsHashValid(string InputStr, string Hash)
		{
			if ( Comparer.Compare( GetHash(InputStr) , Hash.Trim().ToUpper() ) == 0 )
			{
				return true;
			}
			else 
			{
				return false;
			}
		}
/*
		public static void Main()
		{			
			string		Hash			;
			string		Source			;
                        CMd5Hash	Md5	= new		CMd5Hash();

			Source		=	"Hello"	;
			Hash		=	Md5.GetHash( Source ) ;
			System.Console.WriteLine("The MD5 hash of `" + Source + "` is: " + Hash + " .");

			Source		=	"Hello World!" ;
			Hash		=	Md5.GetHash( Source );
			System.Console.WriteLine("The MD5 hash of `" + Source + "` is: " + Hash + " .");

			System.Console.WriteLine("Verifying the hash...");

			if	( Md5.IsHashValid(Source, Hash) ) 
			{
				System.Console.WriteLine("The hashes are the same.");
			} 
			else
			{
				System.Console.WriteLine("The hashes are not same.");
			}

		}
*/
	}
}