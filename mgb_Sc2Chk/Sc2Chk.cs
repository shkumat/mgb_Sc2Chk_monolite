// Версия 2.03 от 07.09.2019г. Для открытия дня - некоторые проверки,которые были в Скрудж-2
// encoding=cp-1251
//
// v2  - Отключена поддержка БД БРЦ
//
/*		Параметры программы :

	-DayDate	Текущая дата - целое число например 42624,
			если не указано, то отчетная дата = сегодня;
	-Mode		Какие проверки делать :
		TODAY		начало дня в таблице "Сегодня";
		Chk_VIA		Very-Important-Archive  проверка архива особо-важных данных;
		Chk_Misc	проверки всякие разные.
		Chk_ISCard	проверка файла для ISCard
		COPY4MSTAT	копирование файла для Мебиус-Стат
*/
using	__	=	MyTypes.CCommon ;
using	money	=	System.Decimal;
using	MyTypes;

public class CVFileReader : CDatReader {
	readonly int[] Header_Field_Size = { 628 };
	readonly int[] Record_Field_Size = { 603 };

	public override bool Open(string FileName, int CharSet) {
		HeaderFieldSize = Header_Field_Size;
		RecordFieldSize = Record_Field_Size;
		return	base.Open(FileName, CharSet);
	}
}

public	class	Sc2Chk {
	static	string		ScroogeDir	=	CAbc.EMPTY;
	static	string		ScroogeOut	=	CAbc.EMPTY;
	static	string		DayOutDir	=	CAbc.EMPTY;
	static	string		ServerName	=	CAbc.EMPTY;
	static	string		DataBase	=	CAbc.EMPTY;
	static	string		ConnectionString=	CAbc.EMPTY;
	//static	string		BrcConnStr	=	CAbc.EMPTY;
	static	bool		MainBank	=	true;
	static	CCommand	Command		;
	static	CRecordSet	RecordSet	;
	static	CConnection	Connection	;

	static	void	Main()  {
		const	bool	DEBUG		=	false;
		CParam		Param		= new	CParam();
		int		Mode		=	0;	// 1=today, 2=chk_via; 3=chk_misc ; 4=copy for MStat
		int		PrevDate	=	0;
		int		DayDate		=	__.Today();
		int		NextDate	=	__.Today()-1;
		string		LogFileName	=	CAbc.EMPTY ;
		//string		REQUEST_GET_BRC_CONNECTIONSTRING	=	" select 'Server='+Ltrim(Rtrim([Server]))+';Database='+Ltrim(Rtrim([Base]))+';Integrated Security=TRUE;' from dbo.sv_Branchs with (NoLock) where kind=3";
		string		REQUEST_GET_BRANCH_KIND			=	" select BranchKind from dbo.vMega_Common_MyBankInfo with ( NoLock ) " ;
		if	( DEBUG ) {
			Mode=4;
		} else
			switch	( Param["MODE"].Trim().ToUpper() ) {
				case	"TODAY": {
					Mode=1;
					break;
				}
				case	"CHK_VIA": {
					Mode=2;
					break;
				}
				case	"CHK_MISC": {
					Mode=3;
					break;
				}
				case	"COPY4MSTAT": {
					Mode=4;
					break;
				}
				case	"CHK_ISCARD": {
					Chk_ISCard();
					return;
				}
				default	: {
					Mode=0;
					break;
				}
			}
		if	( Param["DAYDATE"].Trim() != CAbc.EMPTY )
			if	( __.CInt( Param["DAYDATE"].Trim() ) > 0 )
				DayDate	=	__.CInt( Param["DAYDATE"].Trim() );
		if	( Param["NEXTDATE"].Trim() != CAbc.EMPTY )
			if	( __.CInt( Param["NEXTDATE"].Trim() ) > 0 )
				NextDate	=	__.CInt( Param["NEXTDATE"].Trim() );
		if	( Mode==0 ) {
			__.Print("Не задан режим работы !");
			return;
		}
		// -------------------------------------------------------
		CScrooge2Config	Scrooge2Config	= new	CScrooge2Config();
		if (!Scrooge2Config.IsValid) {
			CCommon.Print( Scrooge2Config.ErrInfo ) ;
			return;
		}
		ScroogeDir	=	(string)Scrooge2Config["Root"].Trim();
		DayOutDir	=	(string)Scrooge2Config["Output"].Trim();
		ScroogeOut	=	ScroogeDir + "\\" + DayOutDir ;
		ServerName	=	(string)Scrooge2Config["Server"].Trim();
		DataBase	=	(string)Scrooge2Config["DataBase"].Trim();
		if( ScroogeDir == null ) {
			CCommon.Print("  Не найдена переменная `Root` в настройках `Скрудж-2` ");
			return;
		}
		if( ServerName == null ) {
			CCommon.Print("  Не найдена переменная `Server` в настройках `Скрудж-2` ");
			return;
		}
		if( DataBase == null ) {
			CCommon.Print("  Не найдена переменная `Database` в настройках `Скрудж-2` ");
			return;
		}
		CCommon.Print("  Беру настройки `Скрудж-2` здесь :  " + ScroogeDir );
		__.Print("  Сервер        :  " + ServerName  );
		__.Print("  База данных   :  " + DataBase + CAbc.CRLF );
		ConnectionString	=	"Server="	+	ServerName
					+	";Database="	+	DataBase
					+	";Integrated Security=TRUE;"
					;
		// - - - - - - - - - - - - - - - - - - - - - - - - - - - -
		if	( DayOutDir !=	null ) {
			DayOutDir=	ScroogeDir + "\\" + DayOutDir.Trim();
			if	( ! CCommon.DirExists( DayOutDir ) )
				CCommon.MkDir( DayOutDir );
			if	( CCommon.DirExists( DayOutDir ) ) {
				DayOutDir	+=	"\\" + CCommon.StrD( DayDate , 8 , 8 ).Replace("/","").Replace(".","");
				if	( ! CCommon.DirExists( DayOutDir ) )
					CCommon.MkDir( DayOutDir );
				if	( ! CCommon.DirExists( DayOutDir ) )
					DayOutDir	=	ScroogeDir + "\\" ;
				}
			LogFileName		=	DayOutDir + "\\" + "dayopen.log" ;
		}
		else
			LogFileName		=	ScroogeDir + "\\" + "dayopen.log" ;
		Err.LogTo( LogFileName );
		// --------------------------------------------------------------
		Connection		= new	CConnection( ConnectionString ) ;
		if      ( ! Connection.IsOpen() ) {
			__.Print("  Ошибка подключения к серверу !");
			return;
		}
		Command         = new   CCommand( Connection );
		__.Print("Определяю режим банк\\филиал.");
		int	BranchKind	=	( int ) CCommon.IsNull( Command.GetScalar( REQUEST_GET_BRANCH_KIND ) , -1 ) ;
		if	( BranchKind == -1 )
			__.Print("  Ошибка выполнения команды на сервере !");
		else
			if	(  BranchKind != 0 )
				MainBank	=	false;
		/*	Выборка адреса БД БРЦ
		if	( MainBank ) {
			__.Print("Вычитываю адрес БД БРЦ.");
			BrcConnStr	=	( string ) CCommon.IsNull( Command.GetScalar( REQUEST_GET_BRC_CONNECTIONSTRING ) , CAbc.EMPTY );
		}
		*/
		__.Print("Определяю дату предыдущего рабочего дня.");
		PrevDate	=	( int ) CCommon.IsNull( Command.GetScalar(" exec dbo.Mega_Day_Close;2  " + DayDate.ToString() ) , (int) 0 );
		if	( PrevDate == 0 ) {
			__.Print(" Ошибка определения даты предыдущего рабочего дня !" );
			return;
		}
		else
			__.Print("Предыдущий рабочий день - " + __.StrD( PrevDate , 8, 8 ) );
		switch	( Mode ) {
			case	4 : {
				CopyXML4Mebius( NextDate , "" );
				// CopyXML4Mebius( NextDate , "kv" ); // Это перекладывание киевских отчетов очетов отключено 18 июля 2019г.
				return;
			}
		}
		Command.Close();
		Connection.Close();
		// ------------------------------------------------------
		switch	( Mode ) {
			case	1 : {
				Today( PrevDate , DayDate );
				break;
			}
			case	2 : {
				Chk_VIA( PrevDate
				,	( __.Month( DayDate ) != __.Month( PrevDate ) )
				);
				break;
			}
			case	3 : {
				Chk_Misc( PrevDate , DayDate );
				break;
			}
			default	: {
				__.Print("Неверно указан режим работы !");
				break;
			}
		}
	}
	// ------------------------------------------------------
	static	void	LookForSertificates() {
		Command         = new   CCommand( Connection );
		string	ScInputDir	=	( string ) __.IsNull( Command.GetScalar(" exec dbo.Mega_Day_Open;6 " ) , (string) CAbc.EMPTY );
		ScInputDir	=	ScInputDir.Trim();
		if	( ScInputDir.Length==0  )
			__.Print("Ошибка определения входного каталога для ОДБ.");
		string[]	Sertificates	=	__.GetFileList( ScInputDir + "\\!*.*" );
		if	( Sertificates != null )
			if	( Sertificates.Length>0 )
				CConsole.GetBoxChoice("","Найдены сертификаты открытых ключей от НБУ.","","   Не забудьте загрузить их в `Скрудж`.","");
		Command.Close();
	}
	// ------------------------------------------------------
	static	void	CopyXML4Mebius( int NextDate , string BranchCode ) {
		string	DateStr		=	__.DtoC(NextDate);
        	string  DateStr_	=	DateStr.Substring(0,4)+ "_" +DateStr.Substring(4,2) + "_" + DateStr.Substring(6,2);
		string  SourceName	=	(string) __.IsNull( Command.GetScalar( " exec dbo.Mega_Day_Open;5 @Mode=3 " ) , (string) CAbc.EMPTY );
		string  DestName	=	(string) __.IsNull( Command.GetScalar( " exec dbo.Mega_Day_Open;5 @Mode=4 " ) , (string) CAbc.EMPTY );
		string	BeginOfFileName =	(string) __.IsNull( Command.GetScalar( " exec dbo.Mega_Day_Open;5 @Mode=5 " ) , (string) CAbc.EMPTY );
		SourceName		=	SourceName.ToUpper().Replace("\\REPO3","\\REPO3"+BranchCode);
		SourceName		=	__.AddSlash(SourceName) + DateStr_+"\\" + BeginOfFileName + DateStr + ".xml" ;
		DestName		=	__.AddSlash(DestName) + "\\" + BeginOfFileName + DateStr + BranchCode + ".xml";
		if	( ! __.FileExists(SourceName) )
			CConsole.GetBoxChoice(	"" , "Не найден файл" , "" ,SourceName, "");
		else
			if	( ! __.CopyFile(SourceName,DestName) )
				CConsole.GetBoxChoice(	"" , "Ошибка копирования файла" , "" , SourceName, "в", DestName, "");
			else
				__.Print("Успешно скопирован " + SourceName, "  в  "+ DestName);
	}
	// ------------------------------------------------------
	static	void	Chk_ISCard() {
		string	Path		=	"W:\\ISCard\\FS\\Production\\FileSystem\\PAYfiles" ;
		string[] FileList	=	__.GetFileList( Path + "\\P*.ISS" );
		string	FileName	=	CAbc.EMPTY;
		if	( FileList != null )
			if	( FileList.Length > 0 )
				FileName	=	FileList[0];
		if	( FileName.Length == 0 )
			CConsole.GetBoxChoice(	"" , "Не найдено P-файлов в каталоге" , "" , Path , "");
		else
			CConsole.GetBoxChoice(	"" , "Найден P-файл" , "" , FileName , "");
	}
	// ------------------------------------------------------
	static	void	Chk_VIA( int PrevDate , bool Monthly ) {
		string		FileMask	;
		string[]	FileList	;
		string		DayMask		=	__.StrD( PrevDate , 10 , 10 ).Replace("/","").Replace(".","");
		string		CfgFileName	=	ScroogeDir + CAbc.SLASH + "EXE" + CAbc.SLASH + "GLOBAL.FIL" ;
		CCfgFile	CfgFile		= new	CCfgFile( CfgFileName );
		string		ViaDir		=	CfgFile["IMPORTANTARCHIVE"].ToString().Trim().ToUpper();
		//	Daily    ------------------------------
		FileMask	=	ViaDir + CAbc.SLASH + "DAILY" + CAbc.SLASH + "??????" + DayMask + DayMask + "*.*"  ;
		FileList	=	__.GetFileList( FileMask );
		__.Print( "Архивы особо-важных данных :" , "" );
		if	( FileList != null )
			if	( FileList.Length > 0 )
				foreach	(  string FileName in FileList  ) {
					__.Print( __.GetFileName( FileName ) + "\t" + __.GetFileTime( FileName ).ToString() );
		 		}
		 	else
				__.Print("Ежедневные архивы не найдены  !");
		else
			__.Print("Ежедневные архивы не найдены  !");
		//	Monthly  ------------------------------
		if	( Monthly ) {
			FileMask	=	ViaDir + CAbc.SLASH + "MONTHLY" + CAbc.SLASH + "??????01" + DayMask.Substring(2,6) + "*.*"  ;
			FileList	=	__.GetFileList( FileMask );
			__.Print( "" );
			if	( FileList != null )
				if	( FileList.Length > 0 )
					foreach	(  string FileName in FileList  ) {
						__.Print( __.GetFileName( FileName ) + "\t" + __.GetFileTime( FileName ).ToString() );
		 			}
		 		else
					__.Print("Ежемесячные архивы не найдены  !");
			else
				__.Print("Ежемесячные архивы не найдены  !");
		}
		__.Print("","Нажмите Enter для продолжения...");
		__.Input();
	}
	// ------------------------------------------------------
	static	void	Today( int PrevDate , int DayDate ) {
		int	NeedRecalc	=	0;
		string	ScInputDir	=	CAbc.EMPTY;
		Connection		= new	CConnection( ConnectionString ) ;
		if      ( ! Connection.IsOpen() ) {
			__.Print("  Ошибка подключения к серверу !");
			return;
		}
		LookForSertificates();
		__.Print("Ищу обороты за выходные дни.");
		RecordSet		= new	CRecordSet( Connection );
		byte	SavedColor	=	CConsole.BoxColor;
		CConsole.BoxColor       =       CConsole.RED*16 + CConsole.WHITE;
		if	( RecordSet.Open( " exec dbo.Mega_Day_Open;8 " ) )
			while	( RecordSet.Read() )
				CConsole.GetBoxChoice("","Обнаружены проводки за выходной день " + __.StrD( __.CInt( RecordSet[0] ) ,8, 8 ),"");
		RecordSet.Close();
		CConsole.BoxColor       =       SavedColor;
		Command         = new   CCommand( Connection );
		Command.Timeout	=	599 ;
		__.Print("Добавляю в таблицу 'Сегодня' новый день " + __.StrD( PrevDate , 8, 8 ) );
		if	( ! Command.Execute(" exec dbo.Mega_Day_Open;1 " + PrevDate.ToString() + " , 0 " ) )
			__.Print("Ошибка выполнения команды на сервере !");
		__.Print("Снимаю запреты с даты " + __.StrD( DayDate , 8, 8 ) );
		if	( ! Command.Execute(" exec dbo.Mega_Day_Open;10  " + DayDate.ToString() ) )
			__.Print("Ошибка выполнения команды на сервере !");
		if	( MainBank ) {
			__.Print("Переставляю дату работы БРЦ и EMP. " );
			if	( ! Command.Execute(" exec  dbo.Mega_Day_Open;11  " + DayDate.ToString() ) )
				__.Print("Ошибка выполнения команды на сервере !");
		}
		if	( DayDate > ( PrevDate + 1 ) )
			for	( int Date = ( PrevDate + 1 ) ; Date < DayDate ; Date++ ) {
				__.Print("Копирую курсы валют в дату " + __.StrD( Date , 8 , 8 ) );
				if	( ! Command.Execute(" exec dbo.Mega_Day_Open;2 " + Date.ToString() ) )
					__.Print("Ошибка выполнения команды на сервере !");
				__.Print("Добавляю в таблицу 'Сегодня' новый день  " + __.StrD( Date , 8, 8 ) );
				NeedRecalc	=	( int ) __.IsNull( Command.GetScalar( " exec dbo.Mega_Day_Open;1 " + Date.ToString() + ",1,0x14703 ") , (int) 0 );
				if	( NeedRecalc > 0 ) {
					__.Print("Пересчитываю день "+__.StrD( Date , 8 , 8 ) + "..." );
					if	( ! Command.Execute(" exec dbo.SC_EndOfDay " + Date.ToString() + " , 0 , 0 " ) )
						__.Print("Ошибка выполнения команды на сервере !");
				}
			}
		//--------------------------------------------------------
		Command.Close();
		Connection.Close();
		/* На БРЦ снимаем запреты с даты
		if	(  MainBank ) {
			Connection		= new	CConnection( BrcConnStr ) ;
			if      ( ! Connection.IsOpen() ) {
				__.Print("  Ошибка подключения к БРЦ !");
				return;
			}
			Command         = new   CCommand( Connection );
			__.Print("На БРЦ снимаю запреты с даты " + __.StrD( DayDate , 8, 8 ) );
			if	( ! Command.Execute(" update dbo.SV_Today set Flag=0 where DayDate=  " + DayDate.ToString() ) )
				__.Print("Ошибка выполнения команды на сервере !");
			if	( ! Command.Execute(" exec  dbo.Mega_Day_Open;11  " + DayDate.ToString() ) )
				__.Print("Ошибка выполнения команды на сервере !");
			Command.Close();
			Connection.Close();
		}
		*/
	}
	// ------------------------------------------------------
	static	void	Chk_Misc( int PrevDate , int DayDate ) {
		int		Kind		=	0;
		int		OldKind		=	0;
		money		Acc1200Main	=	0;
		//money		Acc1200Brc	=	0;
		money		AccVFile	=	0;
		string		BugInfo		=	CAbc.EMPTY;
		string		BugName		=	CAbc.EMPTY;
		CTextWriter     TextWriter	= new	CTextWriter();
		CConnection	Connection2	=	null	;
		CRecordSet	RecordSet2	=	null	;
		string		DELIMITER1	=	"--------------------------------+--------------------------------------------" + CAbc.CRLF ;
		string		DELIMITER2	=	"-----------+--------------+-----------------------" + CAbc.CRLF ;
		string		DELIMITER3	=	"--------------------------------+-------------+------------------+------------------+-------------------------" + CAbc.CRLF ;
		string		DELIMITER4	=	"--------------+--------------+--------------------+--------------------+--------------------------------------" + CAbc.CRLF ;
		string		DELIMITER5	=	"---------------------------------------------------+------------------" + CAbc.CRLF  ;
		string		DELIMITER6	=	"...................................................." + CAbc.CRLF  ;
		string		ReportFileName	=	ScroogeOut + "\\" + __.StrD( DayDate , 10 , 10 ).Replace("/","").Replace(".","");
		string		VFileName	=	__.StrD( PrevDate , 8 , 8 ).Substring(6,2)
						+	__.StrD( PrevDate , 8 , 8 ).Substring(3,2)
						+	__.StrD( PrevDate , 8 , 8 ).Substring(0,2);
		TextWriter.Create( ReportFileName + ".err", CAbc.CHARSET_DOS );
		TextWriter.Add( __.Now().ToString() + CAbc.CRLF  );
		TextWriter.Close();
		// -------------------------------------------------------------
		Connection		= new	CConnection( ConnectionString ) ;
		if      ( ! Connection.IsOpen() ) {
			__.Print("  Ошибка подключения к серверу !");
			return;
		}
		LookForSertificates();
		RecordSet		= new	CRecordSet( Connection );
		RecordSet.Timeout	=	599 ;
		// -------------------------------------------------------------
		if	( ! MainBank )
			goto	CHECK_MISC;
		// -------------------------------------------------------------
		if	( RecordSet.Open(" exec dbo.Mega_Day_Open;7 " ) )
			if	( RecordSet.Read() )
				VFileName		=	RecordSet[0].Trim() + "\\" + VFileName ;
		VFileName	+=	 "\\$V*.G*" ;
		string[]	VFiles	=	__.GetFileList( VFileName );
		string		Tmps	=	CAbc.EMPTY;
		VFileName	=	CAbc.EMPTY;
		if	( VFiles != null )
			if	( VFiles.Length > 0 )
				VFileName	=	VFiles[ VFiles.Length - 1 ];
		if	( VFileName != CAbc.EMPTY ) {
			CVFileReader	VFileReader	= new	CVFileReader();
			if	( VFileReader.Open( VFileName , CAbc.CHARSET_DOS ) )
				if	( VFileReader.Read() )
					AccVFile	=	__.CCur( VFileReader.Head().Substring(264,16) ) / 100 ;
			VFileReader.Close();
		}
		// -------------------------------------------------------------
		__.Print( " Результаты проверки сводного баланса - в " + ReportFileName + ".err"  );
		if	( RecordSet.Open( " exec  dbo.Mega_Check_Balance;2  @FromDate = " + PrevDate.ToString() + " , @ToDate = " + PrevDate.ToString()  ) )
			if	( RecordSet.Read() ) {
				TextWriter.OpenForAppend( ReportFileName + ".err", CAbc.CHARSET_DOS );
				TextWriter.Add( CAbc.CRLF , CAbc.CRLF , " ОШИБКИ ПРИ ПРОВЕРКЕ СВОДНОГО БАЛАНСА "  + CAbc.CRLF );
				TextWriter.Add( DELIMITER6 , CAbc.CRLF );
				do
					TextWriter.Add( RecordSet[0] , CAbc.CRLF );
				while	( RecordSet.Read() ) ;
				TextWriter.Add( DELIMITER6 , CAbc.CRLF , CAbc.CRLF );
				TextWriter.Close();
			}
		// -------------------------------------------------------------
		__.Print( " Сверяю корсчета на ГК и БРЦ " );
		if	( RecordSet.Open(" exec  Mega_Day_Close;4 " + PrevDate.ToString() ) )
			if	( RecordSet.Read() )
				Acc1200Main	=	__.CCur( RecordSet[0] );
		/*	Сверка корсчета с БРЦ
		Connection2		= new	CConnection( BrcConnStr );
		if      (  Connection.IsOpen() ) {
			RecordSet2	= new	CRecordSet( Connection2 );
			if	( RecordSet2.Open(" exec  Mega_Day_Close;4 " + PrevDate.ToString() ) )
				if	( RecordSet2.Read() )
					Acc1200Brc	=	__.CCur( RecordSet2[0] );
			if	( Acc1200Brc == 0 )
				__.Print(" Ошибка вычитки корсчета на БРЦ ");
			else
				if	( Acc1200Brc != Acc1200Main )
					CConsole.GetBoxChoice("","Неравенство корсчетов на ГК и БРЦ !","");
			RecordSet2.Close();
		}
		else
			__.Print("  Ошибка подключения к БРЦ !");
		Connection2.Close();
		*/
		TextWriter.OpenForAppend( ReportFileName + ".err", CAbc.CHARSET_DOS );
		TextWriter.Add( CAbc.CRLF + "..... Сверка корсчетов ....." + CAbc.CRLF );
		TextWriter.Add( "Корсчет на конец " + __.StrD( PrevDate , 8 , 8 ) + " ( ГК     ) =" + __.StrM( Acc1200Main , 19 ) + CAbc.CRLF );
		// TextWriter.Add( "Корсчет на конец " + __.StrD( PrevDate , 8 , 8 ) + " ( БРЦ    ) =" + __.StrM( Acc1200Brc , 19 ) + CAbc.CRLF  );
		if	( AccVFile != 0 )
			TextWriter.Add( "Корсчет по последнему V-файлу        =" + __.StrM( AccVFile , 19 ) + CAbc.CRLF );
		TextWriter.Add(  "........................................................." + CAbc.CRLF );
		TextWriter.Close();
		// -------------------------------------------------------------
		__.Print( " Вывожу черный список ЕМА в файл " + ReportFileName + ".ema"  );
		if	( RecordSet.Open( " exec dbo.Mega_CheckEMA_StopList " ) )
			if	( RecordSet.Read() ) {
				TextWriter.OpenForAppend( ReportFileName + ".ema", CAbc.CHARSET_DOS );
				TextWriter.Add( CAbc.CRLF + " Клиенты, включеные в черный список ЕМА "  + CAbc.CRLF );
				TextWriter.Add( " По состоянию на " + __.Now().ToString() + CAbc.CRLF );
				TextWriter.Add( DELIMITER2 );
				TextWriter.Add(" Внутр.код | Идент.код кл.| Имя клиента " + CAbc.CRLF );
				TextWriter.Add( DELIMITER2 );
				do {
					TextWriter.Add( __.Left( RecordSet[1] , 11 ) ) ;
					TextWriter.Add( "|" );
					TextWriter.Add( __.Left( RecordSet[0] , 14 ) ) ;
					TextWriter.Add( "|" );
					TextWriter.Add( __.Left( __.FixUkrI( RecordSet[2] ) , 64 ) ) ;
					TextWriter.Add( CAbc.CRLF );
				} while	( RecordSet.Read() ) ;
				TextWriter.Add( DELIMITER2 );
				TextWriter.Close();
			}
		// -------------------------------------------------------------
	CHECK_MISC:
		__.Print( " Выполняются проверки дня " + __.StrD( PrevDate , 8 , 8 ) + "  (Check_Misc) ..."  );
		if	( RecordSet.Open( " exec dbo.Mega_Check_Misc " + PrevDate.ToString() ) )
			if	( RecordSet.Read() ) {
				TextWriter.OpenForAppend( ReportFileName + ".err", CAbc.CHARSET_DOS );
				TextWriter.Add( CAbc.CRLF + " Ошибки,обнаруженные при проверке дня " + __.StrD( PrevDate , 8 , 8 ) + CAbc.CRLF );
				TextWriter.Add( CAbc.CRLF );
				do {
					Kind		=	__.CInt( RecordSet[0] );
					BugInfo		=	RecordSet[ 2 ];
					BugName		=	__.FixUkrI( RecordSet[ 3 ] );
					if	( ( OldKind > 0 ) && ( OldKind == Kind ) )
						BugInfo		=	CAbc.EMPTY;
					else
						TextWriter.Add( DELIMITER1 );
					OldKind		=	Kind;
					TextWriter.Add( __.Left( BugInfo , 32 ) );
					TextWriter.Add( "|" );
					TextWriter.Add( __.Left( BugName , 44 ) );
					TextWriter.Add( CAbc.CRLF );
				} while	( RecordSet.Read() ) ;
				TextWriter.Add( DELIMITER1 );
				TextWriter.Add( CAbc.CRLF );
				TextWriter.Close();
			}
		// ----------------------------------------------------------
		__.Print( " Выполняются проверки дня " + __.StrD( PrevDate , 8 , 8 ) + "  (Check_Heap) ..."  );
		if	( RecordSet.Open( " exec dbo.Mega_Check_Heap " + PrevDate.ToString() ) )
			if	( RecordSet.Read() ) {
				TextWriter.OpenForAppend( ReportFileName + ".err" , CAbc.CHARSET_DOS );
				TextWriter.Add( " ...дополнительные проверки... " );
				TextWriter.Add( CAbc.CRLF );
				do {
					Kind		=	__.CInt( RecordSet[0] );
					BugInfo		=	RecordSet[ 2 ];
					BugName		=	__.FixUkrI( RecordSet[ 3 ] );
					if	( ( OldKind > 0 ) && ( OldKind == Kind ) )
						BugInfo		=	CAbc.EMPTY;
					else
						TextWriter.Add( DELIMITER1 );
					OldKind		=	Kind;
					TextWriter.Add( __.Left( BugInfo , 32 ) );
					TextWriter.Add( "|" );
					TextWriter.Add( __.Left( BugName , 44 ) );
					TextWriter.Add( CAbc.CRLF );
				} while	( RecordSet.Read() ) ;
				TextWriter.Add( DELIMITER1 );
				TextWriter.Add( CAbc.CRLF );
				TextWriter.Close();
			}
		// ----------------------------------------------------------
		if	( ! MainBank )
			goto	END_OF_PROC;
		// ----------------------------------------------------------
		__.Print( " Ошибки в настройках регуляных платежей - в " + ReportFileName + ".err"  );
		if	( RecordSet.Open( " exec  dbo.Mega_Pst_ERC_CheckReport" ) )
			if	( RecordSet.Read() ) {
				TextWriter.OpenForAppend( ReportFileName + ".err", CAbc.CHARSET_DOS );
				TextWriter.Add( CAbc.CRLF + " Ошибки в настройках регуляных платежей "  + CAbc.CRLF );
				TextWriter.Add( DELIMITER3 );
				TextWriter.Add( "          Название ошибки       | Код клиента |   Номер счета    |      Название клиента " + CAbc.CRLF );
				TextWriter.Add( DELIMITER3 );
				do {
					TextWriter.Add( __.Left( RecordSet["ErrorText"] , 32 ) ) ;
					TextWriter.Add( "|" );
					TextWriter.Add( __.Left( RecordSet["Code"] , 12 ) ) ;
					TextWriter.Add( " | " );
					TextWriter.Add( __.Left( RecordSet["Moniker"] , 17 ) ) ;
					TextWriter.Add( "| " );
					TextWriter.Add( __.FixUkrI( RecordSet["Name"] ) ) ;
					TextWriter.Add( CAbc.CRLF );
				} while	( RecordSet.Read() ) ;
				TextWriter.Add( DELIMITER3 );
				TextWriter.Add( CAbc.CRLF );
				TextWriter.Close();
			}
		// --------------------------------------------------------
		__.Print( " Ошибки в договорах залогов - в  " + ReportFileName + ".err"  );
		if	( RecordSet.Open( " exec  dbo.Mega_CheckTreatyPawnDate " ) )
			if	( RecordSet.Read() ) {
				TextWriter.OpenForAppend( ReportFileName + ".err", CAbc.CHARSET_DOS );
				TextWriter.Add( CAbc.CRLF + CAbc.CRLF + " Ошибки в договорах залогов "  + CAbc.CRLF );
				TextWriter.Add( DELIMITER4 );
				TextWriter.Add( "  Id договора |   Id залога  |   Номер договора   |    Номер залога    |   Название клиента" + CAbc.CRLF );
				TextWriter.Add( DELIMITER4 );
				do {
					TextWriter.Add( __.Right( RecordSet[0] , 13 ) ) ;
					TextWriter.Add( " |" );
					TextWriter.Add( __.Right( RecordSet[3] , 13 ) ) ;
					TextWriter.Add( " |" );
					TextWriter.Add( __.Left( RecordSet[1] , 20 ) ) ;
					TextWriter.Add( "|" );
					TextWriter.Add( __.Left( RecordSet[4] , 20 ) ) ;
					TextWriter.Add( "|" );
					TextWriter.Add( __.FixUkrI( RecordSet[2] ) ) ;
					TextWriter.Add( CAbc.CRLF );
				} while	( RecordSet.Read() ) ;
				TextWriter.Add( DELIMITER4 );
				TextWriter.Add( CAbc.CRLF );
				TextWriter.Close();
			}
		// --------------------------------------------------------
		__.Print( " Ведомость невынесенных на просрочку - в " + ReportFileName + ".err"  );
		if	( RecordSet.Open( " exec  dbo.Mega_Treaty_Check_Prosroch_Control " ) )
			if	( RecordSet.Read() ) {
				TextWriter.OpenForAppend( ReportFileName + ".err", CAbc.CHARSET_DOS );
				TextWriter.Add( CAbc.CRLF + " Не вынесенные на просрочку договора "  + CAbc.CRLF );
				TextWriter.Add( DELIMITER5 );
				TextWriter.Add( "                Описание ошибки                    ¦ ID договора " + CAbc.CRLF );
				TextWriter.Add( DELIMITER5 );
				do {
					TextWriter.Add( __.Left( RecordSet[0] , 51 ) ) ;
					TextWriter.Add( "| " );
					TextWriter.Add( __.Right( RecordSet[1] , 13 ) ) ;
					TextWriter.Add( CAbc.CRLF );
				} while	( RecordSet.Read() ) ;
				TextWriter.Add( DELIMITER5 );
				TextWriter.Add( CAbc.CRLF );
				TextWriter.Close();
			}
		// --------------------------------------------------------
	END_OF_PROC:
			RecordSet.Close();
			Connection.Close();
	}
}