// Версия 2.01  от 27 июня 2017г. Постороение выписок в формате Скрудж-2
//
// v2 - добавлена поддержка IBAN
//
using	money	=	System.Decimal	;
using	__	=	MyTypes.CCommon;
using	MyTypes;

public	class	CSc2Extract {

	const string	EmptyM		=	"              .  ";
	const string	FiveSpaces	=	"     ";
	const string	OneSpace	=	" ";
	int		TODAY		=	__.Today();
	string		CRLF		=	CAbc.CRLF;
	// Параметры построения выписки ://FOLD00
	public	int	DateFrom	=	__.Today() - 1	//	Дата начальная
	,		DateInto	=	__.Today() - 1	//	Дата конечная
	,		OverMode	=	2		//	Режим вывода переоценки
	,		BranchId	=	0		//	Номер филиала
	,		GroupId		=	0		//	Номер группы счетов
	,		UserId		=	0		//	Номер исполнителя
	,		SortMode	=	0		//	Режим сортировки : 0= по сумме ; 1= по типу операции
	;
	public bool	CbMode		=	false		//	Режим банк(false) / клиент-банк(true)
	,		CoolSum		=	false		//	Форматировать сумму с разделителем
	,		AllAmounts	=	false		//	Режим - по всем счетам
	,		ApartFile	=	false		//	Отдельными файлами по каджому исполнителю
	,		NeedPrintMsg	=	false		//	Нужно ли выводить на консоль сообщение
	;
	public	string	Path		=	""		//	Каталог, в который записывать выписки
	,		AccCode		=	""		//	Маска счета
	,		CurrencyTag	=	""		//	Код валюты
	,		ClientCode	=	""		//	Код клиента

	;//FOLD00
	//  Локальыне переменные класса//FOLD00
	CTextReader	TextReader	= new	CTextReader()
	;
	CTextWriter	TextWriter1	= new	CTextWriter()
	,		TextWriter2	= new	CTextWriter()
	;
	CConnection	Conn1		=	null
	,		Conn2		=	null
	;
	CRecordSet	Amounts		=	null
	,		Accounts	=	null
	,		BankInfo	=	null
	,		Transfers	=	null
	;
	bool		IsCorrect	=	false	// Признак того, что начата секция корректирующих проводок
	,		NeedNewFileCreate=	false
	;
	int		TotalMain	=	0
	,		TotalCur	=	0
	;
	long		OldBranchId	=	-1
	,		OldUserId	=	-1
	;
	string		BankCode	=	""
	,		BankName	=	""
	,		MainCrncyTag	=	""
	,		FileName1	=	""
	,		FileName2	=	""
	,		TmpFileName1	=	""
	,		TmpFileName2	=	""
	;
	money		TotalMainDeb	=	0
	,		TotalMainCre	=	0
	,		TotalCurDeb	=	0
	,		TotalCurCre	=	0
	,		TotalEqDeb	=	0
	,		TotalEqCre	=	0
	;
        long		Accounts_Id		;
        long		Accounts_BranchId	;
        int		Accounts_AccCount	;
	int		Accounts_Depth		;
        long		Accounts_RootId		;
        long		Accounts_UserId		;
        long		Accounts_ExtId		;
	string		Accounts_ExtCode	;
	string		Accounts_ExtTag		;
	string		Accounts_Code		;
	string		Accounts_Tag		;
	string		Accounts_Path		;

	long		Amounts_Id         	;
	string		Amounts_Tag        	;
	int		Amounts_Kind       	;
	string		Amounts_IBAN		;
	string		Amounts_Name       	;
	string		Amounts_StateCode  	;
	string		Amounts_ClientName 	;
	long		Amounts_UserId     	;
	string		Amounts_UserName   	;
	int		Amounts_LastDate   	;
	int		Amounts_RestDate   	;
	int		Amounts_DateInto   	;
	money		Amounts_MainBefore 	;
	money		Amounts_MainAfter  	;
	money		Amounts_CrncyBefore	;
	money		Amounts_CrncyAfter 	;
	int		Amounts_Pieces     	;
	money		Amounts_RateBefore 	;
	money		Amounts_RateAfter  	;
	int		Amounts_DateBefore 	;
	int		Amounts_DateAfter  	;
	money		Amounts_SubRate    	;

	long		Transfers_Id		;
	int		Transfers_Side		;
	int		Transfers_DayDate	;
	long		Transfers_ActionId	;
	int		Transfers_OrgDate	;
	string		Transfers_DocCode	;
	string		Transfers_Ctrls		;
	string		Transfers_IBAN		;
	money		Transfers_MainAmount	;
	money		Transfers_CrncyAmount	;
	string		Transfers_CurrencyTag	;
	string		Transfers_BankCode	;
	string		Transfers_BankName	;
	string		Transfers_Code		;
	string		Transfers_Name		;
	string		Transfers_Purpose	;
	string		Transfers_StateCode	;
	int		Transfers_IsCorrect	;
	int		Transfers_Accepted	;
//FOLD00

	public	void	Close() {
		if	( Conn1 != null ) {
			Conn1.Close();
			Conn1 = null;
		}
		if	( Conn2 != null ) {
			Conn2.Close();
			Conn2 = null;
		}
	}

	public	bool	Open( string ConnectionString ) {
		Close();
		if	( ConnectionString == null )
			return	false;
		ConnectionString	=	ConnectionString.Trim();
		if	( ConnectionString == "" )
			return	false;
		Conn1		= new CConnection( ConnectionString );
		if	( ! Conn1.IsOpen() )
			return	false;
		Conn2		= new CConnection( ConnectionString );
		if	( ! Conn2.IsOpen() )
			return	false;
		return	true;
	}

	string	GetCmdText( int Step ) {//fold00
		AccCode		=	AccCode.Trim();
		ClientCode	=	ClientCode.Trim();
		CurrencyTag	=	CurrencyTag.Trim().ToUpper();
		string	Result	=	" exec  dbo.Mega_Extract";
		switch	( Step ) {
			case	1: {
					Result	+=	";1  "
						+	"   @DateFrom="	+	DateFrom.ToString()
						+	" , @DateInto="	+	DateInto.ToString()
						+	" , @OverMode="	+	( OverMode & 3 ).ToString()
						+	" , @OverDate="	+	DateInto.ToString()
						+	" , @FlgTag="	+	( ( CurrencyTag == "" ) ? "0" : "1")
						+	" , @CbMode="	+	( ( CbMode ) ? "2" : "1" )
						+	" , @UserId="	+	UserId.ToString()
						+	" , @ClientCode='"+	ClientCode + "'"
						+	" , @BranchId="	+	BranchId.ToString()
						+	" , @GroupId="	+	GroupId.ToString()
						+	" , @AmountCode='" +	__.GetCodeByMoniker(AccCode)+"%'"
						+	" , @CurrencyTag='" +	( ( CurrencyTag == "UAH" ) ? "" : CurrencyTag ) + "'"
						+	" , @AllAmounts=" +	( ( AllAmounts ) ? "1" : "0" )
						+	" , @AmountMode= 0 , @IsExpected=0 "
						;
					break;
			}
			case	2: {
					Result	+=	";4  "
						+	"   @DateFrom="	+	DateFrom.ToString()
						+	" , @DateInto="	+	DateInto.ToString()
						+	" , @OverMode="	+	( OverMode & 3 ).ToString()
						+	" , @OverDate="	+	DateInto.ToString()
						+	" , @AmountId="	+	Accounts_ExtId.ToString()
						+	" , @ShowSubRate=1 , @Mode=1 , @Depth=1 "
						;
					break;
			}
			case	3: {
					int	Date	=	Amounts_LastDate;
					if	( __.Month( Date ) < __.Month( DateFrom ) )
						Date	=	__.GetDate( __.Year( DateFrom ) + "/" + __.Month( DateFrom ).ToString("00") + "/01" ) - 1;
					Result	+=	";5  "
						+	"   @DateFrom="	+	( Date + 1 ).ToString()
						+	" , @DateInto="	+	Amounts_DateInto.ToString()
						+	" , @AmountId="	+	Accounts_RootId.ToString()
						+	" , @SortMode="	+	( SortMode & 7).ToString()
						+	" , @IsRight=0 , @IsExpected=0 "
						;
					break;
			}
			case	4: {
					Result	+=	( ( ( DateFrom==DateInto ) && (DateFrom==TODAY)  ) ? ";6 " : ";5 " )
						+	"   @DateFrom="	+	DateFrom.ToString()
						+	" , @DateInto="	+	DateInto.ToString()
						+	" , @AmountId="	+	Accounts_ExtId.ToString()
						+	" , @SortMode="	+	( SortMode & 7).ToString()
						+	" , @IsRight=0 , @IsExpected=0 "
						;
					break;
			}
		}
		return	Result;
	}//FOLD00
					//  Процедура построения выписки//FOLD00
	public	bool	Build() {
		bool		Result		=	false;
		long		OldRootId	=	0;
		OldBranchId	=	-1;
		OldUserId	=	-1;
		FileName1	=	"";
		FileName2	=	"";
		NeedNewFileCreate=	true;
		Accounts		= new	CRecordSet( Conn1 );
		if	(  Accounts.Open( GetCmdText( 1 ) ) ) {
			if	(  Accounts.Read() ) {
				CArray	AccountList	= new	CArray();
				do {
					MapAccountFields();
					AccountList.Add(
						__.Left( Accounts_Code				,	32 )
					+	__.Left( Accounts_Tag				,	8  )
					+	__.Left( Accounts_Id.ToString().Trim()		,	20 )
					+	__.Left( Accounts_ExtId.ToString().Trim()	,	20 )
					+	__.Left( Accounts_RootId.ToString().Trim()	,	20 )
					+	__.Left( Accounts_UserId.ToString().Trim()	,	20 )
					+	__.Left( Accounts_BranchId.ToString().Trim()	,	20 )
					+	Accounts_Path
					);
				} while	( Accounts.Read() ) ;
				Accounts.Close();
				BankInfo	= new	CRecordSet( Conn1 );
				if	( BankInfo.Open(" select Code,Name,MainCrncyTag from dbo.vMega_Common_MyBankInfo ") ) {
					if	( BankInfo.Read() ) {
						BankCode	=		BankInfo["Code"].Trim() ;
						BankName	=__.FixUkrI(	BankInfo["Name"] );
						MainCrncyTag	=		BankInfo["MainCrncyTag"].Trim();
					}
				}
				BankInfo.Close();
				BankInfo	=	null;
				Amounts		= new	CRecordSet( Conn1 );
				Transfers	= new	CRecordSet( Conn2 );
				foreach	( string Account in AccountList ) {
					Accounts_Code	=	Account.Substring(0,32).Trim();
					Accounts_Tag	=	Account.Substring(32,8 ).Trim();
					Accounts_Id	= __.CLng(Account.Substring(40,20 ).Trim() );
					Accounts_ExtId	= __.CLng(Account.Substring(60,20 ).Trim() );
					Accounts_RootId	= __.CLng(Account.Substring(80,20 ).Trim() );
					Accounts_UserId	= __.CLng(Account.Substring(100,20 ).Trim() );
					Accounts_BranchId=__.CLng(Account.Substring(120,20 ).Trim() );
					Accounts_Path	=	Account.Substring( 140 );
					OpenOutputStreams();
					if	( Accounts_RootId == 0 ) {
						PrintAccount();
					}
					if	(	( Accounts_RootId != 0 )
						&&	( Accounts_RootId != OldRootId )
						) {
						OldRootId	=	Accounts_RootId;
						PrintAccount();
					}
				};
				AccountList.Clear();
				AccountList=null;
				CloseOutputStreams();
				Transfers.Close();
				Amounts.Close();
				Result=true;
			}
			else
				Accounts.Close();
		}
		else
			Accounts.Close();
		Transfers	=	null;
		Accounts	=	null;
		Amounts		=	null;
		return	Result;
	}//FOLD00

	void	CloseOutputStreams(){//FOLD00
		if	( TextWriter1 != null ) {
			TextWriter1.Close();
		}
		if	( TextWriter2 != null ) {
			TextWriter2.Close();
		}
		if	(	( FileName1 == null )
			||	( FileName2 == null )
			||	( TmpFileName1 == null )
			||	( TmpFileName2 == null )
			)
			return;
		if	( ( FileName1 != "" ) && ( TmpFileName1 != "" ) ) {
			if	( TextReader.Open( TmpFileName1 , CAbc.CHARSET_DOS ) )
				if ( TextReader.Read() ) {
					TextWriter1	= new	CTextWriter();
					if	( TextWriter1.OpenForAppend( FileName1 , CAbc.CHARSET_DOS ) )
						do
							TextWriter1.Add( TextReader.Value , CRLF );
						while	( TextReader.Read() );
					TextWriter1.Close();
				}
			TextReader.Close();
			__.DeleteFile( TmpFileName1 );
			TmpFileName1="";
		}
		if	( ( FileName2 != "" ) && ( TmpFileName2 != "" ) ) {
			if	( TextReader.Open( TmpFileName2 , CAbc.CHARSET_DOS ) )
				if	( TextReader.Read()  ) {
					TextWriter2	= new	CTextWriter();
					if	( TextWriter2.OpenForAppend( FileName2 , CAbc.CHARSET_DOS ) )
						do
							TextWriter2.Add( TextReader.Value , CRLF );
						while	( TextReader.Read() );
					TextWriter2.Close();
					}
			TextReader.Close();
			__.DeleteFile( TmpFileName2 );
			TmpFileName2="";
		}
	}//FOLD00

	void	OpenOutputStreams() {//FOLD00
		string	Suffix	=	"";
		if	( Path == null )
			Path="";
		else
			Path=Path.Trim();
		if	( __.IsEmpty( Path ) )
			Path	= Accounts_Path	+ "\\";
		if	( ( CbMode ) && ( Accounts_BranchId != OldBranchId ) ) {
			if	( OldBranchId != -1 )
				CloseOutputStreams();
			OldBranchId=Accounts_BranchId;

			Suffix		=	"" ;
			FileName1	=	"";
			FileName2	=	"";
			if	( NeedPrintMsg )
				System.Console.Write( "Филиал " );
				System.Console.Write(__.StrI( (int) Accounts_BranchId , 10 ) );
				System.Console.Write(" . " );
			NeedNewFileCreate=true;
		}
		else {
			if	( ( ApartFile ) && ( ! CbMode ) && ( Accounts_UserId != OldUserId) ) {
				if	( OldUserId != -1 )
					CloseOutputStreams();
				OldUserId	=	Accounts_UserId;
				Suffix		=	"_U" + 	Accounts_UserId.ToString("00000").Trim()+"";
				FileName1	=	"";
				FileName2	=	"";
				if	( NeedPrintMsg )
					System.Console.Write( "Исполнитель " );
					System.Console.Write(__.StrI( (int) Accounts_UserId , 5 ) );
					System.Console.Write(" . " );
				NeedNewFileCreate=	true;
			}
		}
		if	( NeedNewFileCreate ) {
			if	( ( FileName1 == ""  ) || ( FileName2 == "" ) ) {
				if	( DateFrom==DateInto )
					FileName1	=	__.DtoC( DateFrom );
				else
					FileName1	=	__.StrD( DateFrom , 8 , 8 ).Replace("/","").Substring(0,4)
						+	__.StrD( DateInto , 8 , 8 ).Replace("/","").Substring(0,4);
				FileName2	=	Path + '\\' + FileName1 + Suffix + ".CXT";
				FileName1	=	Path + '\\' + FileName1 + Suffix + ".EXT";
			}
			TmpFileName1	=	__.GetTempName();
			TmpFileName2	=	__.GetTempName();
			TextWriter1.Create( TmpFileName1 , CAbc.CHARSET_DOS );
			TextWriter2.Create( TmpFileName2 , CAbc.CHARSET_DOS );
			if	( NeedPrintMsg )
				if	( Path=="" )
					__.Print( "Вывод в текущий каталог." );
				else
					__.Print( "Вывод в каталог " + Path );
			NeedNewFileCreate=false;
		}
	}//FOLD00

	string	StrM( money M ) {//fold00
        	if	( CoolSum )
			return	__.StrM( M , 17 );
		else
			return	__.StrN( M , 17 );
	}//FOLD00

	string	StrF( money M ) {//fold00
		return	__.Right( M.ToString().Replace(",",".") , 13 );
	}//FOLD00

	void	PrintAccount() {//FOLD00
		bool	NeedTotals=true;
		if	( Amounts.Open( GetCmdText( 2 ) ) ) {
			if	(  Amounts.Read() ) {
				do {
					TotalMain	=	0;
					TotalMainDeb	=	0;
					TotalMainCre	=	0;
					TotalCur	=	0;
					TotalCurDeb	=	0;
					TotalCurCre	=	0;
					TotalEqDeb	=	0;
					TotalEqCre	=	0;
					IsCorrect	=	false;
					NeedTotals	=	false;
					MapAmountFields();
					PrintHead();
					if	( Accounts_RootId != 0 )			// Если счет валютный
						if	( Transfers.Open( GetCmdText( 3 ) ) )	// тогда выводим переоценку
							if	( Transfers.Read() ) {
								do{
									MapTransferFields();
									PrintOverVal();
									NeedTotals=true;
					 			} while	( Transfers.Read() );
								o("-------------------------------------------------- ----------------- ----------------- ----------------- -----------------",CRLF);
								o("Проводок",__.StrI(TotalMain,6),__.Replicate(OneSpace,37),EmptyM,OneSpace,EmptyM,OneSpace,StrM(TotalMainDeb),OneSpace,StrM(TotalMainCre),CRLF,CRLF);
							}
					if	( Transfers.Open( GetCmdText( 4 ) ) )
						if	( Transfers.Read() ) {
							do{
								MapTransferFields();
								PrintTransfer();
								NeedTotals=true;
				 			} while	( Transfers.Read() );
						}
					if	( NeedTotals || AllAmounts )
						PrintTotals();
				} while	( Amounts.Read() );
			}
		}
	}//FOLD00

	void o( params string[] SList ) {//fold00
		if	( SList == null)
			return;
		if	( SList.Length == 0)
			return;
		foreach	( string Str in SList)
			if	( Str != null )
				if	( Accounts_Tag == ""  )
					TextWriter1.Add( Str );
				else
					TextWriter2.Add( Str );
	}//FOLD00

	void	MapAccountFields(){//fold00
		Accounts_Id		=	__.CLng(	Accounts["Id"          ]);
		Accounts_BranchId	=	__.CLng(	Accounts["BranchId"    ]);
		Accounts_AccCount	=	__.CInt(	Accounts["AccCount"    ]);
		Accounts_Depth		=	__.CInt(	Accounts["Depth"       ]);
		Accounts_RootId		=	__.CLng(	Accounts["RootId"      ]);
		Accounts_UserId		=	__.CLng(	Accounts["UserId"      ]);
		Accounts_ExtId		=	__.CLng(	Accounts["ExtId"       ]);
		Accounts_ExtCode	=	__.GetMonikerByCode( Accounts["ExtCode"]);
		Accounts_ExtTag		=			Accounts["ExtTag"].Trim();
		Accounts_Code		=	__.GetMonikerByCode(Accounts["Code"    ]);
		Accounts_Tag		=			Accounts["Tag"         ].Trim()	;
		Accounts_Path		=			Accounts["Path"        ].Trim()	;
	}//FOLD00

	void	MapAmountFields(){//fold00
		Amounts_Id		=	__.CLng(	Amounts["Id"           ]);
		Amounts_Tag		=			Amounts["Tag"].Trim()	;
		Amounts_IBAN		=			Amounts["IBAN"].Trim()	;
		Amounts_Kind		=	__.CInt(	Amounts["Kind"         ]);
		Amounts_StateCode	=			Amounts["StateCode"    ].Trim() ;
		Amounts_ClientName	=	__.FixUkrI(	Amounts["ClientName"   ]);
		Amounts_UserId		=	__.CInt(	Amounts["UserId"       ]);
		Amounts_UserName	=	__.FixUkrI(	Amounts["UserName"     ]);
		Amounts_Name		=	__.FixUkrI(	Amounts["Name"         ]);
		Amounts_LastDate	=	__.CInt(	Amounts["LastDate"     ]);
		Amounts_RestDate	=	__.CInt(	Amounts["RestDate"     ]);
		Amounts_DateInto	=	__.CInt(	Amounts["DateInto"     ]);
		Amounts_MainBefore	=	__.CCur(	Amounts["MainBefore"   ]);
		Amounts_MainAfter	=	__.CCur(	Amounts["MainAfter"    ]);
		Amounts_CrncyBefore	=	__.CCur(	Amounts["CrncyBefore"  ]);
		Amounts_CrncyAfter	=	__.CCur(	Amounts["CrncyAfter"   ]);
		Amounts_Pieces		=	__.CInt(	Amounts["Pieces"       ]);
		Amounts_RateBefore	=	__.CCur(	Amounts["RateBefore"   ]);
		Amounts_RateAfter	=	__.CCur(	Amounts["RateAfter"    ]);
		Amounts_DateBefore	=	__.CInt(	Amounts["DateBefore"   ]);
		Amounts_DateAfter	=	__.CInt(	Amounts["DateAfter"    ]);
		Amounts_SubRate		=	__.CCur(	Amounts["SubRate"      ]);
	}//FOLD00

	void	MapTransferFields(){//fold00
		Transfers_Id		=	__.CLng(	Transfers["Id"         ]);
		Transfers_Side		=       __.CInt(	Transfers["Side"       ]);
		Transfers_DayDate	=       __.CInt(	Transfers["DayDate"    ]);
		Transfers_ActionId	=       __.CLng(	Transfers["ActionId"   ]);
		Transfers_OrgDate	=       __.CInt(	Transfers["OrgDate"    ]);
		Transfers_DocCode	=       		Transfers["DocCode"    ].Trim();
		Transfers_Ctrls		=       		Transfers["Ctrls"      ].Trim();
		Transfers_IBAN		=       		Transfers["IBAN"       ].Trim();
		Transfers_MainAmount	=       __.CCur(	Transfers["MainAmount" ]);
		Transfers_CrncyAmount	=       __.CCur(	Transfers["CrncyAmount"]);
		Transfers_CurrencyTag	=       		Transfers["CurrencyTag"].Trim();
		Transfers_BankCode	=       		Transfers["BankCode"   ].Trim()   ;
		Transfers_BankName	=       __.FixUkrI(	Transfers["BankName"   ]);
		Transfers_Code		=       __.GetMonikerByCode( Transfers["Code"  ]);
		Transfers_Name		=       __.FixUkrI(	Transfers["Name"       ]);
		Transfers_Purpose	=       __.FixUkrI(	Transfers["Purpose"    ]);
		Transfers_StateCode	=       		Transfers["StateCode"  ].Trim()  ;
		Transfers_IsCorrect	=	__.CInt(	Transfers["IsCorrect"  ] );
	}//FOLD00

	void	PrintHead() {//fold00
		if	( Accounts_RootId == 0 ) {
			o( CRLF , "Виконавець" , __.StrI( Amounts_UserId , 5 ) , OneSpace , __.Right( Amounts_UserName , 61) , CRLF ) ;
			if	( Amounts_IBAN.Length > 0 )
				o( CRLF , "IBAN " , __.Right( Amounts_IBAN , 72) );
			o( CRLF , "Банк" , __.Right( BankCode , 10 ) , OneSpace , __.Right( BankName , 62) );
			o( CRLF , "ЕДРПО " , __.Right( Amounts_StateCode,10 ) , OneSpace , __.Right( "Рахунок " + Accounts_Code + " UAH" ,60 ) );
			if	( Amounts_Name.Length > 71 )
				o( CRLF , "Назва " , Amounts_Name.Substring( 0 ,  70 ) , CRLF);
			else
				o( CRLF , "Назва " , __.Right(  Amounts_Name , 71) , CRLF);
			if	( DateFrom == DateInto ) {
				o(CRLF , "                 Виписка/Особовий рахунок за " , __.StrD( DateInto ,8, 8 ) , CRLF , CRLF );
				o("Документ Банк   Рахунок        ЕДРПУ                  Дебет            Кредит" , CRLF );
				o("-------- ------ -------------- ---------- ----------------- -----------------" , CRLF );
			}
			else {
				o(CRLF , "           Виписка/Особовий рахунок з ", __.StrD(DateFrom,8,8) ," по " , __.StrD(DateInto,8,8) , CRLF , CRLF );
				o("Дата     Документ Банк   Рахунок        ЕДРПУ                  Дебет            Кредит" , CRLF );
				o("-------- -------- ------ -------------- ---------- ----------------- -----------------" , CRLF );
			}
		}
		else	{
			o( CRLF , "Виконавець" , __.StrI( Amounts_UserId , 5 ) , OneSpace , __.Right( Amounts_UserName , 106) , CRLF ) ;
			if	( Amounts_IBAN.Length > 0 )
				o( CRLF , "IBAN " , __.Right( Amounts_IBAN , 117) );
			o( CRLF , "Банк" , __.Right( BankCode , 10 ) , OneSpace , __.Right( BankName , 107) );
			int	xPos	=	Accounts_Code.IndexOf(".");
			if	( xPos > 1 ) {	// Это субсчет
				o( CRLF , "ЕДРПО " , __.Right( Amounts_StateCode,10 ) , OneSpace , __.Right( "Рахунок " + Accounts_Code.Substring( 0 , xPos ) + OneSpace + Amounts_Tag , 105 ) );
				o( CRLF , "Назва ", __.Right( Amounts_Name , 116) , CRLF);
				o( CRLF , "Субрахунок ", __.Right( Accounts_Code , 110) , CRLF);
			}
			else {
				o( CRLF , "ЕДРПО " , __.Right( Amounts_StateCode,10 ) , OneSpace , __.Right( "Рахунок " + Accounts_Code + OneSpace  + Amounts_Tag , 105 ) );
				o( CRLF , "Назва ", __.Right( Amounts_Name , 116) , CRLF);
			}
			o(CRLF , "                                   Виписка/Особовий рахунок з ", __.StrD(DateFrom,8,8) ," по " , __.StrD(DateInto,8,8) , CRLF , CRLF );
			o("Дата     Документ Банк   Рахунок        ЕДРПУ                  Дебет            Кредит             Дебет            Кредит" , CRLF );
			o("-------- -------- ------ -------------- ---------- ----------------- ----------------- ----------------- -----------------" , CRLF );
		}
	}//FOLD00

	void	PrintPurpose( bool ShortMode ) {//fold00
		if	( Transfers_IsCorrect > 0 )
			o(	"Коpигується вихiдний залишок за дату : " + __.StrD( Transfers_OrgDate , 8 , 8 ) , CRLF );
		if	( ShortMode ) {
			o(         "ПРЗН:" ,	__.SubStr( Transfers_Purpose,0,35 ) , CRLF );
			if	( Transfers_Purpose.Length > 36 )
				o( FiveSpaces ,	__.SubStr( Transfers_Purpose,36,71 ) , CRLF );
			if	( Transfers_Purpose.Length > 72 )
				o( FiveSpaces ,	__.SubStr( Transfers_Purpose,72,107 ) , CRLF );
			if	( Transfers_Purpose.Length > 107 )
				o( FiveSpaces ,	__.SubStr( Transfers_Purpose,108,143 ) , CRLF );
			if	( Transfers_Purpose.Length > 144 )
				o( FiveSpaces ,	__.SubStr( Transfers_Purpose,144,179 ) , CRLF );
			if	( Transfers_Purpose.Length > 180 )
				o( FiveSpaces ,	__.SubStr( Transfers_Purpose,180,215 ) , CRLF );
			if	( Transfers_Purpose.Length > 216 )
				o( FiveSpaces ,	__.SubStr( Transfers_Purpose,216,239 ) , CRLF );
			if	( Transfers_IBAN.Length > 0 )
				o( "IBAN: " ,	__.SubStr( Transfers_IBAN , 0 , 32 ) , CRLF );
			if	( ( Transfers_BankCode != "" ) && ( Transfers_BankCode != BankCode) )
				o( "БАНК:" ,	__.SubStr( Transfers_BankName , 0 , 35 ) , CRLF );
			o(         "КРСП:" ,	__.SubStr( Transfers_Name , 0 , 35 ) , CRLF ) ;
			if	( Transfers_Name.Length > 36 )
				o( FiveSpaces ,	__.SubStr( Transfers_Name,36,71 ) , CRLF ) ;
		}
		else {
			o(         "ПРЗН:" ,	__.SubStr( Transfers_Purpose,0,44 ) , CRLF );
			if	( Transfers_Purpose.Length > 45 )
				o( FiveSpaces ,	__.SubStr( Transfers_Purpose,45,89 ) , CRLF );
			if	( Transfers_Purpose.Length > 90 )
				o( FiveSpaces ,	__.SubStr( Transfers_Purpose,90,134 ) , CRLF );
			if	( Transfers_Purpose.Length > 135 )
				o( FiveSpaces ,	__.SubStr( Transfers_Purpose,135,179 ) , CRLF );
			if	( Transfers_Purpose.Length > 180 )
				o( FiveSpaces ,	__.SubStr( Transfers_Purpose,180,224 ) , CRLF );
			if	( Transfers_Purpose.Length > 225 )
				o( FiveSpaces ,	__.SubStr( Transfers_Purpose,225,239 ) , CRLF );
			if	( Transfers_IBAN.Length > 0 )
				o( "IBAN: " ,	__.SubStr( Transfers_IBAN , 0 , 32 ) , CRLF );
			if	( ( Transfers_BankCode != "" ) && ( Transfers_BankCode != BankCode) )
				o( "БАНК:" ,	__.SubStr( Transfers_BankName , 0 , 44 ) , CRLF );
			o(         "КРСП:" ,	__.SubStr( Transfers_Name , 0 , 44 ) , CRLF ) ;
			if	( Transfers_Name.Length > 45 )
				o( FiveSpaces ,	__.SubStr( Transfers_Name,45,79 ) , CRLF ) ;
		}
	}//FOLD00

	void	PrintOverVal() {//fold00
		TotalMain++;
		if	( Transfers_IsCorrect == 1 )
			if	( ! IsCorrect ) {
				IsCorrect=true;
				o(	__.Replicate("-",124)	, CRLF
				,	"                                             Коригуючi проводки"
				,	CRLF , __.Replicate("-",124) , CRLF
				);
			}
		if	( Transfers_Side == 1 ) {
			TotalMainDeb	+=	Transfers_MainAmount;
			o(	__.StrD( Transfers_DayDate , 8 , 8 )	, OneSpace
			,	__.Left( Transfers_DocCode , 8 )	, OneSpace
			,	__.Left( Transfers_BankCode , 6 )	, OneSpace
			,	__.Left( Transfers_Code , 14 )		, OneSpace
			,	__.Left( Transfers_StateCode , 10 )	, OneSpace
			,	EmptyM					, OneSpace
			,	EmptyM					, OneSpace
			,	StrM( Transfers_MainAmount )		, OneSpace
			,	EmptyM					, CRLF
			);
		}
		else {
			TotalMainCre	+=	Transfers_MainAmount;
			o(	__.StrD( Transfers_DayDate , 8 , 8 )	, OneSpace
			,	__.Left( Transfers_DocCode , 8 )	, OneSpace
			,	__.Left( Transfers_BankCode , 6 )	, OneSpace
			,	__.Left( Transfers_Code , 14 )		, OneSpace
			,	__.Left( Transfers_StateCode , 10 )	, OneSpace
			,	EmptyM					, OneSpace
			,	EmptyM					, OneSpace
			,	EmptyM					, OneSpace
			,	StrM( Transfers_MainAmount )		, CRLF
			);
		}
		PrintPurpose( false );
	}//FOLD00

	void	PrintTransfer() {//fold00
		if	( Accounts_RootId == 0  ) {
			TotalMain++;
			if	( Transfers_IsCorrect == 1 )
				if	( ! IsCorrect ) {
					IsCorrect=true;
					if	( DateFrom == DateInto )
						o(	__.Replicate("-",77)	, CRLF
						,	"                        Коригуючi проводки"
						,	CRLF , __.Replicate("-",77) , CRLF
						);
					else
						o(	__.Replicate("-",84)	, CRLF
						,	"                        Коригуючi проводки"
						,	CRLF , __.Replicate("-",84) , CRLF
						);
				}
			if	( DateFrom != DateInto )
				o( __.StrD( Transfers_DayDate , 8 , 8 )	, OneSpace	);
			if	( Transfers_Side == 1 ) {
				TotalMainDeb	+=	Transfers_MainAmount;
				o(	__.Left( Transfers_DocCode , 8 )	, OneSpace
				,	__.Left( Transfers_BankCode , 6 )	, OneSpace
				,	__.Left( Transfers_Code , 14 )		, OneSpace
				,	__.Left( Transfers_StateCode , 10 )	, OneSpace
				,	StrM( Transfers_MainAmount )		, OneSpace
				,	EmptyM					, CRLF
				);
			}
			else {
				TotalMainCre	+=	Transfers_MainAmount;
				o(	__.Left( Transfers_DocCode , 8 )	, OneSpace
				,	__.Left( Transfers_BankCode , 6 )	, OneSpace
				,	__.Left( Transfers_Code , 14 )		, OneSpace
				,	__.Left( Transfers_StateCode , 10 )	, OneSpace
				,	EmptyM					, OneSpace
				,	StrM( Transfers_MainAmount )		, CRLF
				);
			}
			PrintPurpose( DateFrom == DateInto );
		}
		else {
			TotalCur++;
			if	( Transfers_IsCorrect == 1 )
				if	( ! IsCorrect ) {
					IsCorrect=true;
					o(	__.Replicate("-",124)	, CRLF
					,	"                                             Коригуючi проводки"
					,	CRLF , __.Replicate("-",124) , CRLF
					);
				}
			if	( Transfers_Side == 1 ) {
				o(	__.StrD( Transfers_DayDate , 8 , 8 )	, OneSpace
				,	__.Left( Transfers_DocCode , 8 )	, OneSpace
				,	__.Left( Transfers_BankCode , 6 )	, OneSpace
				,	__.Left( Transfers_Code , 14 )		, OneSpace
				,	__.Left( Transfers_StateCode , 10 )	, OneSpace
				,	StrM( Transfers_CrncyAmount )		, OneSpace
				,	EmptyM					, OneSpace
				,	StrM( Transfers_MainAmount )		, OneSpace
				,	EmptyM					, CRLF
				);
				TotalEqDeb	+=	Transfers_MainAmount;
				TotalCurDeb	+=	Transfers_CrncyAmount;
			}
			else {
				o(	__.StrD( Transfers_DayDate , 8 , 8 )	, OneSpace
				,	__.Left( Transfers_DocCode , 8 )	, OneSpace
				,	__.Left( Transfers_BankCode , 6 )	, OneSpace
				,	__.Left( Transfers_Code , 14 )		, OneSpace
				,	__.Left( Transfers_StateCode , 10 )	, OneSpace
				,	EmptyM					, OneSpace
				,	StrM( Transfers_CrncyAmount )		, OneSpace
				,	EmptyM					, OneSpace
				,	StrM( Transfers_MainAmount )		, CRLF
				);
				TotalEqCre	+=	Transfers_MainAmount;
				TotalCurCre	+=	Transfers_CrncyAmount;
			}
			PrintPurpose( false );
		}
	}//FOLD00

	void	PrintTotals() {//fold00
		if	( Accounts_RootId == 0  ) {
			if	( DateFrom == DateInto ) {
				o("========================================= ================= =================",CRLF);
				o("Пpоводок",__.StrI(TotalMain,6),__.Replicate(OneSpace,28),StrM(TotalMainDeb),OneSpace,StrM(TotalMainCre),CRLF);
				o("                                          ----------------- -----------------",CRLF,CRLF);
				if	( Amounts_LastDate > 0 ) {
					if	( Amounts_MainBefore < 0 )
						o("Вхiдний  "+__.StrD(Amounts_LastDate,8,8),__.Replicate(OneSpace,25),StrM(__.Abs(Amounts_MainBefore)),CRLF);
					else
						o("Вхiдний  "+__.StrD(Amounts_LastDate,8,8),__.Replicate(OneSpace,43)+StrM(__.Abs(Amounts_MainBefore)),CRLF);
				}
				if	( Amounts_MainAfter < 0  )
					o("Вихiдний ",__.StrD(Amounts_DateInto,8,8),__.Replicate(OneSpace,25),StrM(__.Abs(Amounts_MainAfter)),CRLF,CRLF);
				else
					o("Вихiдний ",__.StrD(Amounts_DateInto,8,8),__.Replicate(OneSpace,43),StrM(__.Abs(Amounts_MainAfter)),CRLF,CRLF);
				o(__.Replicate("-",77),CRLF,">>",__.Replicate(" =",36)," <<",CRLF,__.Replicate("-",77),CRLF);
			}
			else {
				o("================================================== ================= =================",CRLF);
				o("Пpоводок",__.StrI(TotalMain,6),__.Replicate(OneSpace,37),StrM(TotalMainDeb),OneSpace,StrM(TotalMainCre),CRLF);
				o("                                                   ----------------- -----------------",CRLF,CRLF);
				if	( Amounts_LastDate > 0 ) {
					if	( Amounts_MainBefore < 0 )
						o("Вхiдний  "+__.StrD(Amounts_LastDate,8,8),__.Replicate(OneSpace,34),StrM(__.Abs(Amounts_MainBefore)),CRLF);
					else
						o("Вхiдний  "+__.StrD(Amounts_LastDate,8,8),__.Replicate(OneSpace,52)+StrM(__.Abs(Amounts_MainBefore)),CRLF);
				}
				if	( Amounts_MainAfter < 0  )
					o("Вихiдний ",__.StrD(Amounts_DateInto,8,8),__.Replicate(OneSpace,34),StrM(__.Abs(Amounts_MainAfter)),CRLF,CRLF);
				else
					o("Вихiдний ",__.StrD(Amounts_DateInto,8,8),__.Replicate(OneSpace,52),StrM(__.Abs(Amounts_MainAfter)),CRLF,CRLF);
				o(__.Replicate("-",86),CRLF,">>",__.Replicate(" =",41)," <<",CRLF,__.Replicate("-",86),CRLF);
			}
		}
		else {
			if	( TotalCur > 0 ) {
				o("-------------------------------------------------- ----------------- ----------------- ----------------- -----------------",CRLF);
				o("Проводок",__.StrI(TotalCur,6),__.Replicate(OneSpace,37),StrM(TotalCurDeb),OneSpace,StrM(TotalCurCre),OneSpace,StrM(TotalEqDeb),OneSpace,StrM(TotalEqCre),CRLF,CRLF);
			}
			o("========================================= ========================== ================= ================= =================",CRLF);
			o("Проводок", __.StrI(TotalMain+TotalCur,6) , __.Replicate(OneSpace,37) , StrM(TotalCurDeb) , OneSpace , StrM(TotalCurCre) , OneSpace , StrM(TotalMainDeb+TotalEqDeb) , OneSpace , StrM(TotalMainCre+TotalEqCre) , CRLF);
			if	( OverMode==1 )
				Amounts_RestDate=Amounts_LastDate;Amounts_LastDate=0;
			if	( ( Amounts_LastDate > 0 ) && ( Amounts_LastDate < Amounts_DateInto ) )
				o("Останнiй ", __.StrD(Amounts_LastDate,8,8),"                                  ----------------- ----------------- ----------------- -----------------",CRLF);
			else
				o("                                                   ----------------- ----------------- ----------------- -----------------",CRLF);
			if	( Amounts_RestDate > 0 ) {
				if	( ( Amounts_CrncyBefore < 0 )  )
					o("Вхiдний  ", __.StrD(Amounts_RestDate,8,8),__.Replicate(OneSpace,34),StrM(__.Abs(Amounts_CrncyBefore)),OneSpace,EmptyM,OneSpace,StrM(__.Abs(Amounts_MainBefore)),OneSpace,EmptyM,CRLF);
				else
					o("Вхiдний  ", __.StrD(Amounts_RestDate,8,8), __.Replicate(OneSpace,34),EmptyM,OneSpace,StrM(__.Abs(Amounts_CrncyBefore)),OneSpace,EmptyM,OneSpace,StrM(__.Abs(Amounts_MainBefore)),CRLF);
			}
			if	( Amounts_CrncyAfter < 0 )
				o("Вихiдний ", __.StrD(Amounts_DateInto,8,8),__.Replicate(OneSpace,34),StrM(__.Abs(Amounts_CrncyAfter)),OneSpace,EmptyM,OneSpace,StrM(__.Abs(Amounts_MainAfter)),OneSpace,EmptyM,CRLF);
			else
				o("Вихiдний " , __.StrD(Amounts_DateInto,8,8),__.Replicate(OneSpace,34),EmptyM,OneSpace,StrM(__.Abs(Amounts_CrncyAfter)),OneSpace,EmptyM,OneSpace,StrM(__.Abs(Amounts_MainAfter)),CRLF);
			o( CRLF , "Курс НБУ на " , __.StrD(Amounts_DateBefore,8,8) , " =" , StrF( Amounts_RateBefore / 100 ) , " за " , Amounts_Pieces.ToString(), OneSpace , Amounts_Tag );
			o( CRLF , "Курс НБУ на " , __.StrD(Amounts_DateAfter,8,8) , " ="  , StrF( Amounts_RateAfter / 100 ) , " за ", Amounts_Pieces.ToString() , OneSpace, Amounts_Tag , CRLF );
			o( CRLF , __.Replicate("-",122), CRLF , ">>" , __.Replicate(" =",59) , "<<" , CRLF,__.Replicate("-",122),CRLF);
		}
	}//FOLD00
}
/*
public	class	App {
	public	static void Main() {
		string	ConnectionString	=	"Server=TESTSRV3;Database=SCROOGE70_2;Integrated Security=TRUE;";
		CSc2Extract	Extract		= new	CSc2Extract();
		if	( ! Extract.Open( ConnectionString ) )
			return;
		Extract.DateFrom	=	42854;
		Extract.DateInto	=	42884;
		Extract.OverMode	=	1;
		Extract.NeedPrintMsg	=	true;
		Extract.CbMode		=	false;
		Extract.BranchId	=	0;
		Extract.AccCode		=	"369099782013";
		Extract.Path		=	"D:\\WorkShop\\Sc2Rpt" ;
		Extract.ApartFile	=	false;
		Extract.AllAmounts	=	true;
		Extract.Build();
		Extract.Close();
	}
}
*/