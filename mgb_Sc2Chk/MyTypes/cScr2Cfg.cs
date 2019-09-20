// Версия 1.03 от 17 августа 2017 г. - Вычитка конфигурации Скруджа2 из Global.Cfg
namespace MyTypes
{
	public class	CScrooge2Config
	{
		bool		Is_Valid	;
		string		Err_Info	;
		string		Scrooge2Root;
		CCfgFile	CfgFile		;

		public	bool	IsValid
		{
			get
			{	return Is_Valid;
			}
		}

		public string ErrInfo
		{	
			get
			{	return Err_Info;
			}
		}

		public	string	this[string Key] {
			get {	
				// 2017.08.17 добавлено отсечение комментариев, которые начинаются с символа ;
				string	Result	=	(string) CfgFile[Key.Trim().ToUpper()];
				if	( Result == null )
					return	CAbc.EMPTY;
				if	( Result.IndexOf(';') > 0 )
					Result	=	Result.Substring( 0 , Result.IndexOf(';')-1 );
				return	Result.Trim();
			}
		}

		public	CScrooge2Config()
		{
			Is_Valid	=	false;
			Scrooge2Root	=	CCommon.GetEnvStr("Scrooge2Root") ;
			if	( 	(Scrooge2Root == null) || (Scrooge2Root == "")	)
				{	Err_Info = "Ошибка : Не определена переменная окружения Scrooge2Root !";
					return;
				}
			CfgFile		= new	CCfgFile( Scrooge2Root + "\\EXE\\GLOBAL.CFG" );
			if	(	( CfgFile["SERVER"] == null) || ( CfgFile["SERVER"] == "")	)
				{	Err_Info = "Ошибка чтения параметров из файла "+Scrooge2Root + "\\EXE\\GLOBAL.CFG";
					return;
				}
			Err_Info	=	"";
			Is_Valid	=	true;
		}	

	}
	/*
	public	class	MyDemo
	{
		public static void Main()
		{
			System.Diagnostics.Debugger.Launch();

			СScrooge2Config Scrooge2Config = new СScrooge2Config();

			if ( Scrooge2Config.IsValid )
			{
				System.Console.WriteLine( Scrooge2Config["ROOT"] );
			}
			else
			{
				System.Console.WriteLine( Scrooge2Config.ErrInfo );
			}
		}
	}
	*/
}