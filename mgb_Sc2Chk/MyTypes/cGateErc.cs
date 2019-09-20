// Версия 2.02  от 12 сентября 2014 г.  - Для вычитки конфигурации из Global.Erc

using MyTypes;

namespace MyTypes
{

	//-------------------------------------------------------------
	//              Cтруктура файла с доп.реквизитами от ЕРЦ
	//
	// 1.Название такое же как у !A-файла ,
	//   только расширение начинается с лат. символа E
	//
	// 2.Файл текстовый с длиной строки не более 255 симв.
	//
	// 3.Структура строки ( позиции начинается с 1 )
	//
	//      1..5            -       номер строки исходного А-файла ,
	//                              к которому относится данная строка
	//
	//      6..15           -       кодовое обозначение доп.реквищита ,
	//                              DOCINFO для паспортных данных
	//
	//      16..255         -       значение доп.реквизита
	//
	// 4. Cтруктура реквизита DocInfo :
	//
	// ФИО ; название документа ; серия документа ; номер документа ;
	// кем выдан документ ; когда выдан документ ; адрес ;
	//
	// пример
	//
	// Кругова Любов Василiвна;паспорт;МК;457038;Комiнтрнiвським РВХМУУМВСУ в Харкiвськiй областi 25/03/1997;28.01.1959;Харкiв, вул. Тимурiвцiв 64 кв 10;;
	//
	public sealed class CErcEFileInfo
	{
		public static readonly int HEADER_SIZE = 196;
		public static readonly int RECORD_SIZE = 257;
		public static readonly int[] Header_Field_Size = { 12,6,176,2 };
		public static readonly int[] Record_Field_Size = { 5,10,240,2 };
		//------  Поля заголовка			
		public const int H_FILENAME	= 0;	// char[ 12]; // Название файла
		public const int H_DATE		= 1;	// char[ 6];  // Дата
		public const int H_UNKNOWN	= 2;	// char[174]; // Неизестное содержимое
		public const int H_CRLF		= 3;	// char[  2]; // Символ `конец строки`
		//------  Поля информационной строки			
		public const int L_LINENUM	= 0;	// char[  5]; // Номер строки в исходном файле
		public const int L_PARAMCODE	= 1;	// char[ 10]; // Кодовое название реквизита
		public const int L_INFO		= 2;	// char[240]; // Значение реквизита
		public const int L_CRLF		= 3; 	// char[  2]; // Символ `конец строки`
	}

	public class CErcEReader : CDatReader
	{
		public override bool Open(string FileName, int CharSet)
		{
			HeaderFieldSize = CErcEFileInfo.Header_Field_Size;
			RecordFieldSize = CErcEFileInfo.Record_Field_Size;
			if ((base.Open(FileName, CharSet) == true)) {
				if ((RecordSize == CErcEFileInfo.RECORD_SIZE) & (HeaderSize == CErcEFileInfo.HEADER_SIZE)) {
					return true;
				}
			}
			return false;
		}
		/*
		public static void Main()
		{
			CErcEReader	ErcEReader;
        	        ErcEReader	= new	CErcEReader() ;

			if( ErcEReader.Open("D:\\!e4T_24J.002",CAbc.CHARSET_DOS) )
			{	while (ErcEReader.Read())
				{	CCommon.Print(	ErcEReader[	CErcEFileInfo.L_PARAMCODE	]
						+      " ( "  + ErcEReader[	CErcEFileInfo.L_LINENUM		]
						+      " ) = `"  + ErcEReader[	CErcEFileInfo.L_INFO		] +"`"
						);
					CCommon.Print("");
				}
			}
			ErcEReader.Close();
		}
		*/
	}

	//-------------------------------------------------------------
	public class CErcConfig
	{
		protected string	DirName					;
		protected string	FileName				;
		protected bool		Is_Valid				;
		protected string	Err_Info				;
		protected int		Erc_Date				;
		protected CCfgFile	CfgFile					;
		private	CTextReader	TxtReader				;
		private	CScrooge2Config Scrooge2Config				;

		public bool IsValid()
		{
			return Is_Valid;
		}

		public string ErrInfo()
		{
			return Err_Info;
		}

		public string LogDir()
		{
			return TodayDir() + "\\LOG\\";
		}

		public string TmpDir()
		{
			return TodayDir() + "\\TMP\\";
		}

		public virtual	string StatDir()
		{
			return TodayDir() + "\\SOS\\";
		}

		public	virtual	string	Config_FileName()
		{	return	"\\EXE\\GLOBAL.ERC";
		}

		public	virtual	string TodayDir()
		{	string	TmpS	=	CCommon.DtoC( Erc_Date );
			TmpS	=	TmpS.Substring(6, 2) + TmpS.Substring(4, 2) + TmpS.Substring(2, 2);
			return CfgFile["DaysDir"] + "\\" + TmpS + "\\";
		}

		public string this[string Key]
		{	get
			{	return	(string) CfgFile[ Key.Trim().ToUpper() ];
			}
		}

		public	int	ErcDate()
		{	return Erc_Date;
		}

		public	bool Open( int ErcDate )
		{	Erc_Date	=	ErcDate	;
			TxtReader	= new	CTextReader();
			Scrooge2Config	= new	CScrooge2Config();
			Is_Valid	=	Scrooge2Config.IsValid;
			Err_Info	=	Scrooge2Config.ErrInfo;
			if ( !IsValid() )
				return	false;
			CfgFile		= new	CCfgFile( Scrooge2Config["Root"] + "\\" + Config_FileName() );
			if	(	( CfgFile[ "ConfigDir" ] == null ) 
				||	( CfgFile[ "ConfigDir" ] == "" )
				)
			{	Err_Info = "Ошибка чтения параметров из файла " + Scrooge2Config["Root"] + Config_FileName() ;
				Is_Valid = false;
				return Is_Valid;
			}
			Is_Valid = Validate();
			return Is_Valid; 	
		}

		public bool Validate()
		{
			DirName = (string) CfgFile["InputDir"];
			Err_Info = "Не найден каталог " + DirName;
			if (!CCommon.DirExists(DirName)) {
				if (!CCommon.MkDir(DirName))
					return false;
				if (!CCommon.DirExists(DirName))
					return false;
			}
			// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
			DirName = (string) CfgFile["OutputDir"];
			Err_Info = "Не найден каталог " + DirName;
			if (!CCommon.DirExists(DirName)) {
				if (!CCommon.MkDir(DirName))
					return false;
				if (!CCommon.DirExists(DirName))
					return false;
			}
			// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
			DirName = (string) CfgFile["DaysDir"];
			Err_Info = "Не найден каталог " + DirName;
			if (!CCommon.DirExists(DirName)) {
				if (!CCommon.MkDir(DirName))
					return false;
				if (!CCommon.DirExists(DirName))
					return false;
			}
			// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
			DirName = (string) CfgFile["FlagsDir"];
			Err_Info = "Не найден каталог " + DirName;
			if (!CCommon.DirExists(DirName)) {
				if (!CCommon.MkDir(DirName))
					return false;
				if (!CCommon.DirExists(DirName))
					return false;
			}
			// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
			DirName = (string) CfgFile["ConfigDir"];
			Err_Info = "Не найден каталог " + DirName;
			if (!CCommon.DirExists(DirName)) {
				if (!CCommon.MkDir(DirName))
					return false;
				if (!CCommon.DirExists(DirName))
					return false;
			}
			// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
			DirName = TodayDir();
			Err_Info = "Не найден каталог " + DirName;
			if (!CCommon.DirExists(DirName)) {
				if (!CCommon.MkDir(DirName))
					return false;
				if (!CCommon.DirExists(DirName))
					return false;
			}
			// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
			DirName = StatDir();
			Err_Info = "Не найден каталог " + DirName;
			if (!CCommon.DirExists(DirName)) {
				if (!CCommon.MkDir(DirName))
					return false;
				if (!CCommon.DirExists(DirName))
					return false;
			}
			// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
			DirName = LogDir();
			Err_Info = "Не найден каталог " + DirName;
			if (!CCommon.DirExists(DirName)) {
				if (!CCommon.MkDir(DirName))
					return false;
				if (!CCommon.DirExists(DirName))
					return false;
			}
			// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
			DirName = TmpDir();
			Err_Info = "Не найден каталог " + DirName;
			if (!CCommon.DirExists(DirName)) {
				if (!CCommon.MkDir(DirName))
					return false;
				if (!CCommon.DirExists(DirName))
					return false;
			}
			// - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -			
			Err_Info = "";
			return true;

		}

	}

}
/*
public class App
{
	public static void Main()
	{
		Err.Debug();

		CErcConfig ErcConfig = new CErcConfig();

		if (ErcConfig.IsValid()) {
			System.Console.WriteLine(ErcConfig["InputDir"]);
		} else {
			System.Console.WriteLine(ErcConfig.ErrInfo());
		}
	}
}
*/