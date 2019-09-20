// Версия 1.01 от 8 октября 2015 г. - Классы для работы с DIF-файлами (`Data Interchange Format`)
// Важно : столбцы нумеруются начиная с 1
using	MyTypes;

namespace MyTypes
{

	public	class	CDifWriter	:	CTextWriter ,	IFileOfColumnsWriter {

		string[]BOT		=	{ "-1,0"  , CAbc.CRLF , "BOT" , CAbc.CRLF }
	;	string[]EOD		=	{ "-1,0"  , CAbc.CRLF , "EOD" , CAbc.CRLF }
	;	string[]HEADER		=	{ "TABLE" , CAbc.CRLF , "0,1" , CAbc.CRLF , CAbc.QUOTE , "EXCEL" , CAbc.QUOTE , CAbc.CRLF , "DATA" , CAbc.CRLF , "0,0" , CAbc.CRLF , CAbc.QUOTE , CAbc.QUOTE , CAbc.CRLF }
	;	int	HeaderStatus	=	0
	;	string	CurrentStr,CurrentStr2
	;

		void	IFileOfColumnsWriter.Close() {
			if	( HeaderStatus != 0 )
				base.Add( EOD  );
			base.Close();
		}

		bool	IFileOfColumnsWriter.Write( params string[] MetaData  ) {
			return	Add( MetaData );
		}

		bool	IFileOfColumnsWriter.WriteLine( params string[] MetaData ) {
			bool	Result	=	Add( MetaData );
			HeaderStatus	=	1;
			return	Result;
		}

		bool	IFileOfColumnsWriter.Create(string FileName, int CharSet,  params int[] MetaData) {
			HeaderStatus	=	0;
			return base.Create(FileName, CharSet);
		}

		public	bool Add( params string[] MetaData ) {
			if	( MetaData == null )
					return	true;
			if	( HeaderStatus == 0 ) {
				base.Add( HEADER );
				HeaderStatus=1;
			}
			if	( HeaderStatus == 1 ) {
				base.Add( BOT );
				HeaderStatus=2;
			}
                        for	( int CurrentField=0; CurrentField < MetaData.Length ; CurrentField++ ) {
                        	CurrentStr	=	MetaData[ CurrentField ].Replace( CAbc.QUOTE , CAbc.QUOTE+CAbc.QUOTE );
                        	CurrentStr2	=	CurrentStr.Replace(",","0");	
				if	(  CurrentStr == CAbc.CRLF ) {
					HeaderStatus=1;
					if	( ! base.Add( CAbc.CRLF ) )
						return	false;
				}
				if	( CCommon.IsDigit( CurrentStr2 ) ) {
					if	( ! base.Add( "0," , CurrentStr , CAbc.CRLF , "V" , CAbc.CRLF ) )
						return	false;
				}
				else
					if	( ! base.Add( "1,0" , CAbc.CRLF , CAbc.QUOTE , CurrentStr , CAbc.QUOTE , CAbc.CRLF ) )
						return	false;
			}
			return	true;
		}
	}

	/*
	class	Application
	{
		public static void Main() {

			IFileOfColumnsWriter	Writer	= new	CDifWriter();

			if	( Writer.Create("F:\\Trash\\a.dif" , CAbc.CHARSET_DOS ) ) {
				Writer.Write("11111,2222222");
				Writer.WriteLine("Проверка связи");
				Writer.Write("3333");
				Writer.Write("4444444444");
				Writer.WriteLine();
				Writer.WriteLine("Связь нормальная", "55555");
			}
			Writer.Close();

		}
	}
	*/
}

/*	This is a sample of DIF-files
TABLE
0,1
"EXCEL"
DATA
0,0
""
-1,0
BOT
0,11111,2222222
V
1,0
"Проверка связи"
-1,0
BOT
0,3333
V
0,4444444444
V
-1,0
EOD
*/
