// Версия 1.19  от 13 апреля 2018г. Построение отчетов, аналогичных Скрудж-2.
using	money	=	System.Decimal	;
using	__	=	MyTypes.CCommon;
using	MyTypes;

namespace MyTypes {

public	class	CSc2Reports {
	// shared items//fold00
	CCommand	Command
	;
	CConnection	Conn1
	,		Conn2
	;
	CRecordSet	Amounts
	,		BankInfo
	,		Transfers
	;
	CTextWriter	TextWriter1
	,		TextWriter2
	;
	bool		UseTextWriter2	=	false		;
	const string	OneSpace	=	" "		;
	const string	EmptyM		=	"              .  ";
	string		CRLF		=	CAbc.CRLF	;

	string		Bank_Code	=	CAbc.EMPTY	;
	string		Bank_Name	=	CAbc.EMPTY	;
	string		Bank_EMail	=	CAbc.EMPTY	;
	string		Chief_Name	=	CAbc.EMPTY	;
	string		ChiefAcc_Name	=	CAbc.EMPTY	;
	public	int	Branch_Kind	=	0		;
	public	int	Branch_Id	=	0		;

	string		Amounts_Code				;
	string		Amounts_Tag				;
	string		Amounts_Name				;
	string		Amounts_State				;
	long		Amounts_UserId				;
	string		Amounts_UserName			;
	money		Amounts_MainAmount			;
	money		Amounts_CrncyAmount			;
	int		Amounts_MainLastDate			;
	int		Amounts_CrncyLastDate			;
	money		Amounts_RateBefore			;
	money		Amounts_RateAfter			;
	int		Amounts_Pieces				;
	int		Amounts_Counter				;

	string		Transfers_Code				;
	string		Transfers_Tag				;
	int		Transfers_IsCorrect			;
	int		Transfers_Side				;
	long		Transfers_TransferId			;
	int		Transfers_DayDate			;
	string		Transfers_DocNum			;
	int		Transfers_Kind				;
	string		Transfers_Ctrls				;
	long		Transfers_ActionId			;
	string		Transfers_SourceCode			;
	string		Transfers_TargetCode			;
	string		Transfers_DebetCode			;
	string		Transfers_CreditCode			;
	string		Transfers_DebetName			;
	string		Transfers_CreditName			;
	string		Transfers_DebetState			;
	string		Transfers_CreditState			;
	money		Transfers_MainAmount			;
	money		Transfers_CrncyAmount			;
	string		Transfers_Purpose			;
	string		Transfers_SourceName			;
	string		Transfers_TargetName			;

	string	StrF( money M ) {
		return	__.Right( M.ToString().Replace(",",".") , 13 );
	}

	string	DtoR( int DayDate ) {
		string	Result	=	__.DtoC( DayDate );
		Result		=	Result.Substring(6,2)
				+	Result.Substring(4,2)
				+	Result.Substring(0,4);
		return	Result;
	}

	void o( params string[] SList ) {
		if	( SList == null)
			return;
		if	( SList.Length == 0)
			return;
		foreach	( string Subj in SList)
			if	( UseTextWriter2 )
		 		o2( Subj );
			else
			 	o1( Subj );
	}

	void o1( string Subj ) {
		if	( TextWriter1 != null ) {
			TextWriter1.Add( Subj );
		}
	}

	void o2( string Subj ) {
		if	( TextWriter2 != null ) {
			TextWriter2.Add( Subj );
		}
	}//FOLD00

	public	void	Close() {//fold00
		if	( Conn1 != null ) {
			Conn1.Close();
			Conn1 = null;
		}
		if	( Conn2 != null ) {
			Conn2.Close();
			Conn2 = null;
		}
	}//FOLD00

	public	bool	Open( string ConnectionString ) {//FOLD00
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
		BankInfo	= new	CRecordSet( Conn2 );
		if	( BankInfo.Open(" select Code,Name,EMail,BranchId,BranchKind,ChiefName,ChiefAccName from dbo.vMega_Common_MyBankInfo ") ) {
			if	( BankInfo.Read() ) {
				Bank_Code	=		BankInfo["Code"].Trim()  ;
				Bank_Name	=__.FixUkrI(	BankInfo["Name"].Trim() );
				Bank_EMail	=		BankInfo["EMail"].Trim() ;
				Branch_Id	=__.CInt(	BankInfo["BranchId"].Trim() ) ;
				Branch_Kind	=__.CInt(	BankInfo["BranchKind"].Trim() ) ;
				Chief_Name	=__.FixUkrI(	BankInfo["ChiefName"].Trim() );
				ChiefAcc_Name	=__.FixUkrI(	BankInfo["ChiefAccName"].Trim() );
			}
			else
				return	false;
		}
		BankInfo.Close();
		return	true;
	}//FOLD00

	void	MapAmountFields(){//fold00
		Amounts_Code		=	__.GetMonikerByCode(Amounts["Code"].Trim() );
		Amounts_Tag		=			Amounts["Tag"		].Trim();
		Amounts_Name		=	__.FixUkrI(	Amounts["Name"		].Trim() );
		Amounts_State		=			Amounts["State"		].Trim();
		Amounts_UserId		=	__.CLng(	Amounts["UserId"	].Trim() );
		Amounts_UserName	=	__.FixUkrI(	Amounts["UserName"	].Trim() );
		Amounts_MainAmount	=	__.CCur(	Amounts["MainAmount"	].Trim() );
		Amounts_CrncyAmount	=	__.CCur(	Amounts["CrncyAmount"	].Trim() );
		Amounts_MainLastDate	=	__.CInt(	Amounts["MainLastDate"	].Trim() );
		Amounts_CrncyLastDate	=	__.CInt(	Amounts["CrncyLastDate"	].Trim() );
		Amounts_RateBefore	=	__.CCur(	Amounts["RateBefore"	] ) ;
		Amounts_RateAfter	=	__.CCur(	Amounts["RateAfter"	] ) ;
		Amounts_Pieces		=	__.CInt(	Amounts["Pieces"	].Trim() );
		Amounts_Counter		=	__.CInt(	Amounts["Counter"	].Trim() );
	}//FOLD00

	void	MapTransferFields() {//fold00
		Transfers_Code		=	__.GetMonikerByCode( Transfers["Code"].Trim() )	;
		Transfers_Tag           =			Transfers["Tag"	].Trim()	;
		Transfers_IsCorrect     =	__.CInt(	Transfers["IsCorrect"	].Trim() )	;
		Transfers_Side          =	__.CInt(	Transfers["Side"	].Trim() )	;
		Transfers_TransferId    =	__.CLng(	Transfers["TransferId"	].Trim() )	;
		Transfers_DayDate       =	__.CInt(	Transfers["DayDate"	].Trim() )	;
		Transfers_DocNum        =			Transfers["DocNum"	].Trim()	;
		Transfers_Kind          =	__.CInt(	Transfers["Kind"	].Trim() )	;
		Transfers_Ctrls         =			Transfers["Ctrls"	].Trim()	;
		Transfers_ActionId      =	__.CLng(	Transfers["ActionId"	].Trim() )	;
		Transfers_SourceCode    =			Transfers["SourceCode"	].Trim()	;
		Transfers_TargetCode    =			Transfers["TargetCode"	].Trim()	;
		Transfers_DebetCode     =	__.GetMonikerByCode( Transfers["DebitCode"].Trim() );
		Transfers_CreditCode    =	__.GetMonikerByCode( Transfers["CreditCode"].Trim() );
		Transfers_DebetName     =	__.FixUkrI(	Transfers["DebitName"	].Trim() )	;
		Transfers_CreditName    =	__.FixUkrI(	Transfers["CreditName"	].Trim() )	;
		Transfers_DebetState    =			Transfers["DebitState"	].Trim()	;
		Transfers_CreditState   =			Transfers["CreditState"	].Trim()	;
		Transfers_MainAmount    =	__.CCur(	Transfers["MainAmount"	].Trim() )	;
		Transfers_CrncyAmount   =	__.CCur(	Transfers["CrncyAmount"	].Trim() )	;
		Transfers_Purpose       =	__.FixUkrI(	Transfers["Purpose"	].Trim() )	;
		Transfers_SourceName    =	__.FixUkrI(	Transfers["SourceName"	].Trim() )	;
		Transfers_TargetName    =	__.FixUkrI(	Transfers["TargetName"	].Trim() )	;

		if	( Transfers_DebetCode.IndexOf(".") > 0 )
			Transfers_DebetCode	=	Transfers_DebetCode.Substring(0, Transfers_DebetCode.IndexOf(".") );
		if	( Transfers_CreditCode.IndexOf(".") > 0 )
			Transfers_CreditCode	=	Transfers_CreditCode.Substring(0, Transfers_CreditCode.IndexOf(".") );
	}//FOLD00

	//  Построение документов дня//FOLD00
	public	bool	DocOfDay( int Date , string FileName ) {
		string	SEPARATOR	=	__.Replicate("-",116) + CAbc.CRLF;
		string	HEADER		=	" номер    |           дебет           |       сума     | сума  в валютi |валюта|симв|          кредит" + CAbc.CRLF;
		money	MainAmount	=	0
		,	CrncyAmount	=	0
		,	ActionTotalM	=	0
		,	UserTotalM	=	0
		,	TotalM		=	0	;
		string	Code
		,	Ctrls
		,	ActionName
		,	SourceCode
		,	DebitMoniker
		,	TargetCode
		,	CreditMoniker
		,	CurrencyTag			;
		int	ActionId	=	0
		,	CreatorId	=	0
		,	OldActionId	=	0
		,	OldCreatorId	=	0
		,	ActionTotalC	=	0
		,	UserTotalC	=	0
		,	TotalC		=	0	;
		if	( ! Conn1.IsOpen() )
			return	false;
		CRecordSet	RecordSet	= new	CRecordSet( Conn1 );
		if	( RecordSet.Open(" exec dbo.Mega_DocReestr;2  @FromDate="+Date.ToString()+",@ToDate="+Date.ToString() ) ) {
			TextWriter1	= new	CTextWriter();
			if	( ! TextWriter1.Create( FileName , CAbc.CHARSET_DOS ) ) {
				RecordSet.Close();
				return	false;
			}
			o( CAbc.CRLF , __.Replicate(" ",25) + "Документи дня за " + __.StrD( Date , 8 , 8 ) , CAbc.CRLF , CAbc.CRLF );

			while	( RecordSet.Read() )	{
				Code		=			RecordSet["Code"	].Trim()	;
				Ctrls		=			RecordSet["Ctrls"	].Trim()	;
				ActionId	=	__.CInt(	RecordSet["ActionId"	].Trim() )	;
				CreatorId	=	__.CInt(	RecordSet["CreatorId"	].Trim() )	;
				ActionName	=	__.FixUkrI(	RecordSet["ActionName"	].Trim() )	;
				SourceCode	=			RecordSet["SourceCode"].Trim()		;
				DebitMoniker	=	__.GetMonikerByCode( RecordSet["DebitCode"].Trim() )	;
				TargetCode	=			RecordSet["TargetCode"	].Trim()	;
				CreditMoniker	=	__.GetMonikerByCode( RecordSet["CreditCode"].Trim() )	;
				MainAmount	=	__.CCur(	RecordSet["MainAmount"	].Trim() )	;
				CrncyAmount	=	__.CCur(	RecordSet["CrncyAmount"	].Trim() )	;
				CurrencyTag	=			RecordSet["CurrencyTag"	].Trim()	;
				if	( ( CreatorId != OldCreatorId ) || ( ActionId != OldActionId ) ) {
					if	( OldActionId != 0 )
						o( SEPARATOR , "За користувачем N" , __.StrI( OldCreatorId , 5 ) , " за данним типом операцii всього документiв" , __.StrI( UserTotalC,6 ) ,  " на загальну суму "  , __.StrN( UserTotalM , 17 ) , " гpн. ( в екв. ) " , CAbc.CRLF , CAbc.CRLF );
					UserTotalC	=	0;
					UserTotalM	=	0;
					OldCreatorId	=	CreatorId;
				}
				if	( ActionId != OldActionId ) {
					if	( OldActionId != 0 )
						o( SEPARATOR , "За типом операцii" , __.StrI( OldActionId , 5 )  , " всього документiв" , __.StrI( ActionTotalC,6 ) ,  " на загальну суму "  , __.StrN( ActionTotalM , 17 ) , " гpн. ( в екв. ) " , CAbc.CRLF , CAbc.CRLF );
					ActionTotalC	=	0;
					ActionTotalM	=	0;
					OldActionId	=	ActionId;
					o(	CAbc.CRLF
					,	"      Тип операцii      "
					,	ActionId.ToString()
					,	"   "
					,	ActionName
					,	CAbc.CRLF
					,	SEPARATOR
					,	HEADER
					,	SEPARATOR
					);
				}
				TotalM		+=	MainAmount;
				UserTotalM	+=	MainAmount;
				ActionTotalM	+=	MainAmount;
				TotalC		++	;
				UserTotalC	++	;
				ActionTotalC	++	;
				o(	__.Left( Code , 10 ) , "|"
				,	__.Left( SourceCode , 7 ) , ":"
				,	__.Left( DebitMoniker , 20 )
				,	__.StrN( MainAmount , 16 )  , "|"
				,	__.StrN( CrncyAmount , 16 )  , "| "
				,	__.Left( CurrencyTag , 4 ) , " |"
				,	__.Left( Ctrls , 4 ) ,"|"
				,	__.Left( TargetCode , 6 ) , ":"
				,	__.Left( CreditMoniker , 20 )
				,	"   /                     /       "
				,	CreatorId.ToString()
				,	CAbc.CRLF
				);
			}
			o( SEPARATOR , "За користувачем N" , __.StrI( CreatorId , 5 )  , " за данним типом операцii всього документiв" , __.StrI( UserTotalC , 6 ) ,  " на загальну суму "  , __.StrN( UserTotalM , 17 ) , " гpн. ( в екв. ) " , CAbc.CRLF , CAbc.CRLF );
			o( SEPARATOR , "За типом операцii" , __.StrI( ActionId , 5 )  , " всього документiв" , __.StrI( ActionTotalC , 6 ) ,  " на загальну суму "  , __.StrN( ActionTotalM , 17 ) , " гpн. ( в екв. ) " , CAbc.CRLF , CAbc.CRLF );
			o( CAbc.CRLF , SEPARATOR , "Всього документiв" , __.StrI( TotalC , 6 ) ,  " на загальну суму "  , __.StrN( TotalM , 17 ) , " гpн. ( в екв. ) " , CAbc.CRLF , CAbc.CRLF );
			TextWriter1.Close();
		}
		else {
			RecordSet.Close();
			return	false;
		}
		RecordSet.Close();
		return	true;
	}//FOLD00

	//  Построение оборотно-сальдовой ведомости//fold00
	public	bool	Saldovka(	string	FileName
				,	int	FromDate
				,	int	ToDate
				,	int	CorDate
				,	string	Code
				,	string	ClientCode
				,	string	CurrencyTag
				,	int	BranchId
				,	int	GroupId
				,	int	UserId
				,	byte	HideFlag	// 1=не выводить с нулевыми оборотами ; 2=не выводить с нулевыми остатками+оборотами
			) {
		int	LastDate	=	0
		,	OldTopic	=	0
		,	Topic		=	0;
		string	SEPARATOR	=	"--------------+---+---------------------------+-----------------+-----------------+-----------------+-----------------+" + CAbc.CRLF
		,	HEADER		=	"              |   |                           |              Обороти              |           Вихiдне сальдо          |" + CAbc.CRLF
					+	"    Рахунок   |Вал|      Назва рахунку        |-----------------------------------+-----------------------------------|" + CAbc.CRLF
					+	"              |   |                           |      Дебет      |      Кредит     |      Актив      |      Пасив      |" + CAbc.CRLF
		,	PIPE		=	"|"
		,	StateCode	=	CAbc.EMPTY
		,	Moniker		=	CAbc.EMPTY
		,	Name		=	CAbc.EMPTY
		,	Tag		=	CAbc.EMPTY
		;
		money	MainDebet	=	0
		,	MainCredit	=	0
		,	CrncyDebet	=	0
		,	CrncyCredit	=	0
		,	MainAmount	=	0
		,	MainActive	=	0
		,	MainPassive	=	0
		,	CrncyAmount	=	0
		,	CrncyActive	=	0
		,	CrncyPassive	=	0
		,	SumMainDebet	=	0
		,	SumMainCredit	=	0
		,	SumMainActive	=	0
		,	SumMainPassive	=	0
		,	TotalMainDebet	=	0
		,	TotalMainCredit	=	0
		,	TotalMainActive	=	0
		,	TotalMainPassive=	0
		;
		if	( ! Conn1.IsOpen() )
			return	false;
		CRecordSet	RecordSet	= new	CRecordSet( Conn1 );
		if	( RecordSet.Open(" exec dbo.Mega_Saldo;2 "
					+" @FromDate   = " +	FromDate.ToString()
					+",@ToDate     = " +	ToDate.ToString()
					+",@CorDate    = " +	CorDate.ToString()
					+",@Code       ='" +	Code.Trim()		+ "'"
					+",@ClientCode ='" +	ClientCode.Trim()	+ "'"
					+",@CurrencyTag='" +	CurrencyTag.Trim()	+ "'"
					+",@BranchId   = " +	BranchId.ToString()
					+",@GroupId    = " +	GroupId.ToString()
					+",@UserId     = " +	UserId.ToString()
					+",@Mode       = 1 " ) ) {
			TextWriter1	= new	CTextWriter();
			if	( ! TextWriter1.Create( FileName , CAbc.CHARSET_DOS ) ) {
				RecordSet.Close();
				return	false;
			}
			o( CAbc.CRLF , __.Space( 19 ) , __.Center( Bank_Name ,80 ) );
			o( CAbc.CRLF , __.Space( 19 ) , __.Center( "Оборотно-сальдова вiдомость" , 80 ) );
			o( CAbc.CRLF , __.Space( 19 ) , __.Center( "з " + __.StrD( FromDate , 10, 10 ) +" по " + __.StrD( ToDate , 10 , 10 ) , 80 ) , CAbc.CRLF , CAbc.CRLF );
			o( SEPARATOR , HEADER, SEPARATOR );

			while	( RecordSet.Read() )	{
				Topic		=	__.CInt(	RecordSet["Topic"].Trim() ) ;
				Moniker		=       __.GetMonikerByCode( RecordSet["Code"].Trim() )	;
				Tag		=       		RecordSet["Tag"].Trim()		;
				Name		=       __.FixUkrI(	RecordSet["Name"].Trim() )	;
				StateCode	=       		RecordSet["StateCode"].Trim() 	;
				MainDebet	=       __.CCur(	RecordSet["MainDebet"].Trim() )	;
				MainCredit	=       __.CCur(	RecordSet["MainCredit"].Trim() );
				CrncyDebet	=       __.CCur(	RecordSet["CrncyDebet"].Trim() );
				CrncyCredit	=       __.CCur(	RecordSet["CrncyCredit"].Trim());
				MainAmount	=       __.CCur(	RecordSet["MainAmount"].Trim() );
				CrncyAmount	=       __.CCur(	RecordSet["CrncyAmount"].Trim());
				LastDate	=       __.CInt(	RecordSet["LastDate"].Trim()  )	;
				CrncyActive	=	( CrncyAmount > 0 ) ? 0 : ( 0 - CrncyAmount )  ;
				CrncyPassive	=	( CrncyAmount > 0 ) ? CrncyAmount :  0 ;
				MainActive	=	( MainAmount > 0 ) ? 0 : ( 0 - MainAmount ) ;
				MainPassive	=	( MainAmount > 0 ) ? MainAmount :  0  ;
				if	( Moniker.Substring(0,4).ToUpper() == "ZZZZ" ) {
					if	( __.IsEmpty( Tag ) )
						o(	SEPARATOR , __.StrI( Topic , 4 )
						,	" за валют.|" , __.Left(Tag,3)
						,	"|                           |"
						,	__.StrN( CrncyDebet , 17 ) , PIPE
						,	__.StrN( CrncyCredit , 17 ), PIPE
						,	__.StrN( CrncyActive , 17 ), PIPE
						,	__.StrN( CrncyPassive , 17), PIPE
						,	CAbc.CRLF , SEPARATOR
						);
					else
						o(	SEPARATOR , __.StrI( Topic , 4 )
						,	" за валют.|" , __.Left(Tag,3)
						,	"|                           |"
						,	__.StrN( CrncyDebet , 17 ) , PIPE
						,	__.StrN( CrncyCredit , 17 ), PIPE
						,	__.StrN( CrncyActive , 17 ), PIPE
						,	__.StrN( CrncyPassive , 17), PIPE
						,	CAbc.CRLF , "              |   |                           |"
						,	__.StrN( MainDebet , 17 )  , PIPE
						,	__.StrN( MainCredit , 17 ) , PIPE
						,	__.StrN( MainActive , 17 ) , PIPE
						,	__.StrN( MainPassive , 17) , PIPE
						,	CAbc.CRLF , SEPARATOR
						);
					continue;
				}
				if	(
						( Topic != OldTopic )
					&&	( OldTopic != 0 )
					) {
					o(	__.StrI( OldTopic , 4 )
					,	"  всього  |   |                           |"
					,	__.StrN( SumMainDebet , 17 ) , PIPE
					,	__.StrN( SumMainCredit , 17 ), PIPE
					,	__.StrN( SumMainActive , 17 ), PIPE
					,	__.StrN( SumMainPassive , 17), PIPE
					,	CAbc.CRLF , SEPARATOR
					);
					SumMainDebet	=	0	;
					SumMainCredit	=	0	;
					SumMainActive	=	0	;
					SumMainPassive	=	0	;
				}
				OldTopic	=	Topic		;
				SumMainDebet	+=	MainDebet	;
				SumMainCredit	+=	MainCredit	;
				SumMainActive	+=	MainActive	;
				SumMainPassive	+=	MainPassive	;
				TotalMainDebet	+=	MainDebet	;
				TotalMainCredit	+=	MainCredit	;
				TotalMainActive	+=	MainActive	;
				TotalMainPassive+=	MainPassive	;
				// HideFlag=1 - не выводить с нулевыми оборотами
				if	(	( ( HideFlag & 1 ) != 0 )
					&&	(  MainDebet == 0 )
					&&	(  MainCredit == 0 )
					)
					continue;
				// HideFlag=2 - не выводить с нулевыми оборотами
				if	(	( ( HideFlag & 2 ) != 0 )
					&&	(  MainDebet == 0 )
					&&	(  MainCredit == 0 )
					&&	(  MainActive == 0 )
					&&	(  MainPassive == 0 )
					)
					continue;
				if	( __.IsEmpty( Tag ) )
					o(	__.Left( Moniker , 14 )     , PIPE
					,	__.Left( Tag,3)             , PIPE
					,	__.Left( Name , 27 )        , PIPE
					,	__.StrN( CrncyDebet , 17 )  , PIPE
					,	__.StrN( CrncyCredit , 17 ) , PIPE
					,	__.StrN( CrncyActive , 17 ) , PIPE
					,	__.StrN( CrncyPassive , 17) , PIPE
					,	CAbc.CRLF
					);
				else
					o(	__.Left( Moniker , 14 )     , PIPE
					,	__.Left(Tag,3)              , PIPE
					,	__.Left( Name , 27 )        , PIPE
					,	__.StrN( CrncyDebet , 17 )  , PIPE
					,	__.StrN( CrncyCredit , 17 ) , PIPE
					,	__.StrN( CrncyActive , 17 ) , PIPE
					,	__.StrN( CrncyPassive , 17) , PIPE
					,	CAbc.CRLF ,  "              |   |                           |"
					,	__.StrN( MainDebet , 17 )   , PIPE
					,	__.StrN( MainCredit , 17 )  , PIPE
					,	__.StrN( MainActive , 17 )  , PIPE
					,	__.StrN( MainPassive , 17)  , PIPE
					,	CAbc.CRLF
					);
                        }
			o(	__.StrI( OldTopic , 4 )
			,	"  всього  |   |                           |"
			,	__.StrN( SumMainDebet , 17 ) , PIPE
			,	__.StrN( SumMainCredit , 17 ), PIPE
			,	__.StrN( SumMainActive , 17 ), PIPE
			,	__.StrN( SumMainPassive , 17), PIPE
			,	CAbc.CRLF , SEPARATOR , SEPARATOR
			,	"    Всього    |   |                           |"
			,	__.StrN( TotalMainDebet , 17 ) , PIPE
			,	__.StrN( TotalMainCredit , 17 ), PIPE
			,	__.StrN( TotalMainActive , 17 ), PIPE
			,	__.StrN( TotalMainPassive , 17), PIPE
			,	CAbc.CRLF , SEPARATOR , CAbc.CRLF
			);
			TextWriter1.Close();
		}
		else {
			RecordSet.Close();
			return	false;
		}
		RecordSet.Close();
		return	true;
	}//FOLD00

	//  Построение репозитарного файла//fold00
	public	bool	Repozitory(	string	FileCode
				,	string	Path
				,	int	FromDate
				,	int	ToDate
				,	int	RepoDate
				,	bool	SchemeD
				,	bool	EchoToConsole
			) {
		if	( ! Conn1.IsOpen() )
			return	false;
		string	Region		=	null
		,	OldRegion	=	null
		,	ProcName	=	"dbo.RP_Repozitory_GetFile" + __.Right("00"+FileCode.Trim(),2)
		,	ShortFileName	=	"#"
					+	__.Right( "00" + FileCode.Trim() , 2 )
					+	__.Right("___" + Bank_EMail.ToUpper() , 3 )
					+	__.StrH( __.Month( ToDate ) , 1  )
					+	__.StrY( __.Day( ToDate ) , 1  )
					+	( SchemeD ? ".D" : ".C" )
					+	__.StrY( __.Day( RepoDate ) , 1 )
					+	"1";
		CRecordSet	RecordSet	= new	CRecordSet( Conn1 );
		if	( EchoToConsole )
			CConsole.ShowBox("","Выполняется расчет на сервере.","");
		if	( RecordSet.Open(	"   exec " + ProcName
					+	"   @D1 = " + FromDate.ToString()
					+	" , @D2 = " + ToDate.ToString()
					+	" , @TurnDirection = 2 "
					+	" , @Modes = "  + ( SchemeD ? "8200"  : "8" )
					+	" , @ConfirmedDoc = 0 " ) ) {
			if	( EchoToConsole )
				CConsole.Clear();
			if	( RecordSet.IsEmpty() ) {
				if	( EchoToConsole ) {
					CConsole.ShowBox("","Не найдены расчитанные остатки на дату!","","Для продолжения нажмите Enter.","");
					__.Input();
					CConsole.Clear();
				}
				RecordSet.Close();
				return	false;
			}
			string	FileName	=	Path.Trim() + CAbc.SLASH + ShortFileName ;
			TextWriter1	= new	CTextWriter();
			if	( ! TextWriter1.Create( FileName , CAbc.CHARSET_DOS) ) {
				RecordSet.Close();
				return	false;
			}
			if	( ! TextWriter1.Add( __.Left(CAbc.EMPTY,100) + CAbc.CRLF ) ) {
				RecordSet.Close();
				return	false;
			}
			string	Pointer,Rest;
			CArray	Lines	= new	CArray();
			int	LineCount	=	0;
			while	( RecordSet.Read() ) {
				Pointer	=	RecordSet[0].Trim();
				Rest	=	RecordSet[1].Trim();
				if	( SchemeD ) {
					Region	=	RecordSet[3].Trim();
					if	( Region != OldRegion )  {
						Lines.Add( "#1=" , Region , CAbc.CRLF );
						OldRegion = Region;
					}
				}
				if	( Rest.IndexOf(",") > 0 )
					Rest	=	Rest.Substring(0,Rest.IndexOf(","));
				if	( Rest.IndexOf(".") > 0 )
					Rest	=	Rest.Substring(0,Rest.IndexOf("."));
				Lines.Add( Pointer + "=" + Rest + CAbc.CRLF );
				LineCount++;
			}
			string	Header	=	( SchemeD ? "09" : "03" )
					+	"=" +	DtoR( RepoDate )
					+	"=" +	DtoR( FromDate )
					+	"=" +	DtoR( ToDate )
					+	"=" +	DtoR( __.Today() )
					+	"=" + __.Hour( __.Now() ).ToString("00") + __.Minute( __.Clock() ).ToString("00")
					+	"=" + __.Left( Bank_Code , 6 )
					+	"=" + "21"
					+	"=" + LineCount.ToString("000000000")
					+	"=" +	__.Left( ShortFileName , 12 )
					+	"=      =";
			TextWriter1.Add( __.Left( Header , 148 ) + CAbc.CRLF ) ;
			if	( ! SchemeD )
				TextWriter1.Add( "#1=" + Bank_Code + CAbc.CRLF ) ;
			TextWriter1.Add( Lines ) ;
			TextWriter1.Close();
			if	( EchoToConsole )
				__.Print("Результат записан в файл "+FileName);
		}
		else {
			if	( EchoToConsole ) {
				CConsole.Clear();
				CConsole.ShowBox("","Ошибка выполнения команды на сервере!","","Для продолжения нажмите Enter.","");
				__.Input();
				CConsole.Clear();
			}
		}
		RecordSet.Close();
		return	true;
	}//FOLD00

	//  Кредитовка по клиент-банку, за один день, выводится в указанный файл//fold00
	public	bool	CbCrdDoc( int Date , int BranchId , string FileName ) {
		string		SEPARATOR	=	 __.Replicate("-",77) + "|" + CRLF ;
		string		SPACES		=	 __.Replicate(" ",77) + "|" + CRLF ;

		int		Transfers_OrgDate       ;
	        long		Transfers_ActionId	;
		long		Transfers_CreatorId	;
		string		Transfers_DocNum	;
		string		Transfers_Invert	;
		string		Transfers_Ctrls		;
		string		Transfers_SourceCode	;
		string		Transfers_DebitMoniker	;
		string		Transfers_TargetCode	;
		string		Transfers_CreditMoniker	;
		string		Transfers_CreditInit	;
		money		Transfers_MainAmount	;
		money		Transfers_CrncyAmount	;
		string		Transfers_CurrencyTag	;
		string		Transfers_BankName	;
		long		Transfers_UserNo	;
		string		Transfers_UserName	;
		string		Transfers_AmountName	;
		string		Transfers_Purpose	;
		string		Transfers_DebitName	;
		string		Transfers_CreditName	;
		string		Transfers_DebitState	;
		string		Transfers_CreditState	;

		if	( ! Conn1.IsOpen() )
		return	false;

		Transfers	= new	CRecordSet( Conn1 );

		if	( Transfers.Open(" exec dbo.Mega_Creditovka  @FromDate="+Date.ToString()+",@ToDate="+Date.ToString()+",@BranchId="+BranchId.ToString()+" , @Code='' , @ClientCode='' , @GroupId=0 , @UserId=0 , @AllDocs=1 ") ) {

			TextWriter1	= new	CTextWriter();
			if	( ! TextWriter1.Create( FileName , CAbc.CHARSET_DOS ) ) {
				Transfers.Close();
				return	false;
			}

	      		string	Code		=	"";
			string	BankCode	=	"";
			string	UserCode	=	"";
			int	TotalUserC	=	0;
			money	TotalUserS	=	0;
			int	TotalCodeC	=	0;
			money	TotalCodeS	=	0;
			int	TotalBankC	=	0;
			money	TotalBankS	=	0;

			while	( Transfers.Read() ) {

				Transfers_OrgDate	=	__.CInt(	Transfers["OrgDate"].Trim() )		;
				Transfers_DocNum	=	__.FixDocNum(	Transfers["Code"].Trim() ) 		;
				Transfers_SourceCode	=			Transfers["SourceCode"].Trim()		;
				Transfers_TargetCode	=			Transfers["TargetCode"].Trim()		;
				Transfers_DebitMoniker	=	__.GetMonikerByCode( Transfers["DebitCode"].Trim() )	;
				Transfers_CreditMoniker	=	__.GetMonikerByCode( Transfers["CreditCode"].Trim() )	;
				Transfers_CreditInit	=	__.GetMonikerByCode( Transfers["CreditInit"].Trim() )	;
				Transfers_MainAmount	=	__.CCur(	Transfers["MainAmount" ]	)	;
				Transfers_CrncyAmount	=	__.CCur(	Transfers["CrncyAmount" ]	)	;
				Transfers_BankName	=	__.FixUkrI(	Transfers["BankName"	]	)	;
				Transfers_UserNo	=       __.CInt(	Transfers["UserId"	]	)	;
				Transfers_UserName	=       __.FixUkrI(	Transfers["UserName"	]	)	;
				Transfers_AmountName	=       __.FixUkrI(	Transfers["AmountName"	]	)	;
				Transfers_Purpose	=       __.FixUkrI(	Transfers["Purpose"	]	)	;
				Transfers_DebitName	=       __.FixUkrI(	Transfers["DebitName"	]	)	;
				Transfers_CreditName	=       __.FixUkrI(	Transfers["CreditName"	]	)	;
				Transfers_DebitState	=       		Transfers["DebitState"	]		;
				Transfers_CreditState	=       		Transfers["CreditState"	]		;

				if	( Code != Transfers_CreditMoniker ) {
					if	( Code !="" ) {
						o( SEPARATOR ) ; o(" Всього вiд банку А                                                          |" ); o( CRLF );
						o( __.Left( " " + TotalBankC.ToString() + " документ(iв) на " + __.StrN(TotalBankS,17) + " " , 77) ); o( "|" ); o( CRLF );
						o( SEPARATOR ); o( SPACES ); o( __.Left(" Всього на pахунок " + Code ,77) ); o( "|" ); o( CRLF ); o( SPACES );
						o( __.Right( TotalCodeC.ToString() + " документ(iв) на " + __.StrN( TotalCodeS , 17 ) + " " , 77 ) ); o( "|" ); o( CRLF );
						o( SPACES ); o("              М.П.                                                           |"); o( CRLF );
						o( SPACES ); o("                               пiдпис ________________                       |"); o( CRLF );
						o(SEPARATOR);
					}
					TotalCodeC	=	0;
					TotalCodeS	=	0;
					TotalBankC	=	0;
					TotalBankS	=	0;
					Code		=	Transfers_CreditMoniker;
					BankCode	=	Transfers_SourceCode;
					o(SPACES) ; o( __.Left( "РЕЕСТР КРЕДИТОВЫХ ПЛАТIЖНИХ ДОКУМЕНТIВ ВIД " + __.StrD( Date,8,8) , 77 ) ) ; o( "|" ); o( CRLF ); o( SPACES );
					o( __.Left( "ОТРИМАНИХ ДЛЯ ЗАРАХУВАННЯ КОШТIВ НА РАХУНОК " + Code , 77 ) ); o( "|" ); o( CRLF );
					o( __.Left( Transfers_AmountName,77) ); o( "|" ); o( CRLF ); o( SPACES ); o(SEPARATOR); o( SPACES );
					o( "Рахунок  клiента A           |  Найименовання клiента банка А |       Сума   |"); o( CRLF ); o( SPACES ); o(SEPARATOR);
				}
				else	{
					if	( BankCode != Transfers_SourceCode ) {
						if	( BankCode != "" ) {
							o( SEPARATOR );
							o(" Всього вiд банку А                                                          |"); o( CRLF );
							o( __.Right( TotalBankC.ToString() + " документ(iв) на " + __.StrN( TotalBankS , 17 ) + " " , 77) ); o( "|" ); o( CRLF );
							o( SEPARATOR );
						}
						TotalBankC	=	0;
						TotalBankS	=	0;
						BankCode	=	Transfers_SourceCode;
					}
       				}

				o( SPACES );
				o( __.Left( "Док.N " + Transfers_DocNum + " вiд  " + __.StrD( Transfers_OrgDate , 8 , 8 ) , 44 ) );
				o( __.Right( __.StrN( Transfers_MainAmount , 17 ) + " ", 33 ) ); o( "|" ); o( CRLF );
				o( __.Left( "Банк А:     " + Transfers_SourceCode +"   " + Transfers_BankName , 77 ) ); o( "|" ); o( CRLF );
				o( __.Left( "Рахунок клieнта A: " + Transfers_DebitMoniker.PadRight(20) +". Код ЕДРПОу клieнта A: " + Transfers_DebitState,77)); o( "|" ); o( CRLF );
				o( __.Left( "Найменування клieнта A: " + Transfers_DebitName,77)); o( "|" ); o( CRLF );
				o( __.Left( "Рахунок клieнта Б: " + Transfers_CreditInit.PadRight(20) + ". Код ЕДРПОу клieнта Б: " + Transfers_CreditState,77)); o( "|" ); o( CRLF );
				o( __.Left( "Найменування клieнта Б: " + Transfers_CreditName,77)); o( "|" ); o( CRLF );
				if	( Transfers_Purpose.Length > 0 )
					if	( Transfers_Purpose.Length > 77 ) {
						o( Transfers_Purpose.Substring(0,77) ); o( "|" ); o( CRLF );
						if	( Transfers_Purpose.Length > 154 ) {
							o( Transfers_Purpose.Substring(77,77) ) ; o( "|" ); o( CRLF );
							if	( Transfers_Purpose.Length > 231 ) {
							 	o( Transfers_Purpose.Substring(154,77) ); o( "|" ); o( CRLF );
							}
							else {
								o( __.Left( Transfers_Purpose.Substring(154) , 77 ) ); o( "|" ); o( CRLF );
							}
						}
						else {
							o( __.Left( Transfers_Purpose.Substring(77) , 77 ) ); o( "|" ); o( CRLF );
						}
					}
					else {
						o( __.Left( Transfers_Purpose , 77 ) ) ; o( "|" ); o( CRLF );
					}
				TotalUserC	++	;
				TotalUserS	+=	Transfers_MainAmount;
				TotalCodeC	++	;
				TotalCodeS	+=	Transfers_MainAmount;
				TotalBankC	++	;
				TotalBankS	+=	Transfers_MainAmount;
			}

			o( SEPARATOR ) ; o(" Всього вiд банку А                                                          |" ); o( CRLF );
			o( __.Right( TotalBankC.ToString() + " документ(iв) на " + __.StrN(TotalBankS,17) + " " ,77) ); o( "|" ); o( CRLF );
			o( SEPARATOR ); o( SPACES ); o( __.Left(" Всього на pахунок " + Code ,77)); o( "|" ); o( CRLF ); o( SPACES );
			o( __.Right( TotalCodeC.ToString() + " документ(iв) на " + __.StrN( TotalCodeS,17) + " " , 77 ) ); o( "|" ); o( CRLF );
			o( SPACES ); o("              М.П.                                                           |"); o( CRLF );
			o( SPACES ); o("                               пiдпис ________________                       |"); o( CRLF );
			o(SEPARATOR);

			TextWriter1.Close();
		}
		else	{
			Transfers.Close();
			return	false;
		}
		Transfers.Close();
		return	true;
	}//FOLD00

	//  Выписка по Южкабелю, за один день, выводится в два файла _1 _2//fold00
	public	bool	YuzhCable( int Date , int BranchId , string FileName ) {
		int		VsegoDeb
		,		VsegoCre			;
		money		InpDeb
		,		InpCre
		,		RotDeb
		,		RotCre
		,		OstDeb
		,		OstCre				;
		string		SEPARATOR	=	CRLF + CRLF + "  " + __.Replicate("- ",38);
		string		HORIZONTAL_LINE	=	"  --------+------+--------------+----------+-----------------+-----------------+";
		string		HORIZONTAL_LINE1=	"  " + __.Replicate("=",41) + " " + __.Replicate("=",17) + " " + __.Replicate("=",17);
		string		HORIZONTAL_LINE2=	"  " + __.Replicate(" ",42) + __.Replicate("-",17) + " " + __.Replicate("-",17);

		if	( ! Conn1.IsOpen() )
			return	false;
		Amounts		= new	CRecordSet( Conn1 );

		if	( Amounts.Open( " exec dbo.mega_vipiska;1  @CurrencyTag='UAH',@FromDate="+Date.ToString()+",@ToDate="+Date.ToString()+",@BranchId="+BranchId.ToString() ) ) {

			Transfers	= new	CRecordSet( Conn2 );
			TextWriter1	= new	CTextWriter();
			TextWriter2	= new	CTextWriter();
			TextWriter1.Create( FileName + "_1.txt" , CAbc.CHARSET_DOS );
			TextWriter2.Create( FileName + "_2.txt" , CAbc.CHARSET_DOS );

			while	( Amounts.Read() )	{
				MapAmountFields();
				VsegoDeb		=	0	;
				VsegoCre		=	0	;
				InpDeb			=	0	;
				InpCre			=	0	;
				RotDeb			=	0	;
				RotCre			=	0	;
				OstDeb			=	0	;
				OstCre			=	0	;

				if	( Transfers.Open(" exec dbo.mega_vipiska;2 @FromDate="+Date.ToString()+",@ToDate="+Date.ToString()+",@Code='"+Amounts["Code"]+"'") ) {

					while	( Transfers.Read() ) {
						MapTransferFields();

						if	( Transfers_Side == 1 ) {
						 	if	( VsegoDeb == 0 ) {
								o1( CRLF + "  "+ __.Left( Bank_Name , 50 )+"         Банк" + __.Right(Bank_Code , 14) + CRLF );
								o1("  ОКПО " +  __.Left( Amounts_State , 10 ) +"                 " + __.Right( "Рахунок " + Amounts_Code.Trim() + " " + Amounts_Tag,46) + CRLF );
								o1("  Назва " + __.Right( Amounts_Name , 71 ) + CRLF + CRLF + CRLF );
								o1("                  Реєстр видаткових платежiв за  "+ __.StrD( Date , 8 , 8 ) + CRLF + CRLF + HORIZONTAL_LINE + CRLF );
								o1("  Документ| Банк |   Рахунок    |  ЕДРПОу  |      Дебет      |      Кредит     |" + CRLF );
							}
							VsegoDeb	++	;
							RotDeb		+=	Transfers_CrncyAmount;
							o1( HORIZONTAL_LINE + CRLF );
							o1("  Контрагент: " + __.Left( Transfers_CreditName , 30 ) +  __.StrM( Transfers_CrncyAmount , 17 ) + CRLF );
							o1("  Банк      : " + __.Left( Transfers_TargetName , 64 ) + CRLF );
							o1("  " + __.Left(Transfers_DocNum,8) + " " + __.Left( Transfers_TargetCode , 6 ) + " " + __.Left( Transfers_CreditCode , 14 ) + " " + __.Left( Transfers_CreditState,10) + CRLF );
							if	( Transfers_Purpose.Length > 65  ) {
								o1("  Пpизнач.  : " + Transfers_Purpose.Substring(0,65) + CRLF );
								if	(  Transfers_Purpose.Length > 142 ) {
									o1("  "+ Transfers_Purpose.Substring(65,77).Trim() + CRLF );
									if	( Transfers_Purpose.Length > 219 ) {
										o1("  "+ Transfers_Purpose.Substring(142,77).Trim() + CRLF );
										o1("  "+ Transfers_Purpose.Substring(219).Trim() + CRLF );
									}
									else	{
										o1("  "+ Transfers_Purpose.Substring(142).Trim() + CRLF );
									}
								}
								else	{
									o1("  "+ Transfers_Purpose.Substring(65).Trim() + CRLF );
								}
							}
							else {
								o1("  Пpизнач.  : " + Transfers_Purpose + CRLF );
							}
						}
						else {
							if	( VsegoCre == 0 ) {
								o2( CRLF + "  "+ __.Left(Bank_Name,50)+"         Банк" + __.Right( Bank_Code , 14 ) + CRLF );
								o2("  ОКПО "+ __.Left( Amounts_State,10) +"                 " + __.Right( "Рахунок " + Amounts_Code.Trim() + " " + Amounts_Tag,46) + CRLF );
								o2("  Назва " + __.Right( Amounts_Name , 71 )+ CRLF + CRLF + CRLF );
								o2("                  Реєстр прибуткових платежiв за  "+__.StrD( Date , 8 , 8 ) + CRLF + CRLF + HORIZONTAL_LINE + CRLF );
								o2("  Документ| Банк |   Рахунок    |  ЕДРПОу  |      Дебет      |      Кредит     |" + CRLF );
							}
							VsegoCre	++	;
							RotCre		+=	Transfers_CrncyAmount;
							o2( HORIZONTAL_LINE + CRLF );
							o2("  Контрагент: " + __.Left( Transfers_DebetName , 30 ) +  "                  " + __.StrM( Transfers_CrncyAmount , 17 ) + CRLF );
							o2("  Банк      : " + __.Left( Transfers_SourceName , 64 ) + CRLF );
							o2("  " + __.Left(Transfers_DocNum,8) + " " + __.Left( Transfers_SourceCode , 6 ) + " " + __.Left( Transfers_DebetCode , 14 ) + " " + __.Left( Transfers_DebetState,10) + CRLF );
							if	( Transfers_Purpose.Length > 65  ) {
								o2("  Пpизнач.  : " + Transfers_Purpose.Substring(0,65) + CRLF );
								if	(  Transfers_Purpose.Length > 142 ) {
									o2("  "+ Transfers_Purpose.Substring(65,77).Trim() + CRLF );
									if	( Transfers_Purpose.Length > 219 ) {
										o2("  "+ Transfers_Purpose.Substring(142,77).Trim() + CRLF );
										o2("  "+ Transfers_Purpose.Substring(219).Trim() + CRLF );
									}
									else	{
										o2("  "+ Transfers_Purpose.Substring(142).Trim() + CRLF );
									}
								}
								else	{
									o2("  "+ Transfers_Purpose.Substring(65).Trim() + CRLF );
								}
							}
							else {
								o2("  Пpизнач.  : " + Transfers_Purpose + CRLF );
							}
						}
					}
					OstCre		=	Amounts_MainAmount;
					InpCre		=	OstCre-RotCre+RotDeb;
					OstDeb		=	0;
					InpDeb		=	0;
					if	( OstCre < 0 ) {
						OstDeb		=	0-OstCre;
						OstCre		=	0;
					}
					if	( InpCre < 0 ) {
						InpDeb		=	0-InpCre;
						InpCre		=	0;
					}
					if	( VsegoDeb > 0 ) {
						o1( HORIZONTAL_LINE1 + CRLF );
						o1("  Пpоводок " + __.Right( VsegoDeb.ToString() , 10 ) + "                       " + __.StrM( RotDeb , 17 ) + CRLF + HORIZONTAL_LINE2 + CRLF );
						if	( Amounts_MainLastDate != 0 ) {
							o1("  Останнiй  " + __.StrD(Amounts_MainLastDate,8,8) + __.Replicate(" ",24) + __.StrM(InpDeb,17) + " " + __.StrM(InpCre,17) + CRLF);
						}
						o1("  Вихiдний  " + __.StrD( Date , 8 , 8 )  +  __.Replicate (" ",24) + __.StrM( OstDeb , 17 ) + " " + __.StrM( OstCre , 17 ) + CRLF + SEPARATOR + CRLF );
					}
					if	( VsegoCre > 0 ) {
						o2( HORIZONTAL_LINE1 + CRLF );
						o2("  Пpоводок " + __.Right( VsegoCre.ToString() , 10 ) + "                                         " + __.StrM( RotCre , 17 ) + CRLF + HORIZONTAL_LINE2 + CRLF );
						if	( Amounts_MainLastDate !=0 ) {
							o2("  Останнiй  " + __.StrD( Amounts_MainLastDate , 8 , 8 ) + __.Replicate( " " , 24 ) + __.StrM( InpDeb , 17 ) + " " + __.StrM( InpCre , 17 ) + CRLF );
						}
						o2("  Вихiдний  " + __.StrD( Date , 8 , 8 )  +  __.Replicate (" ",24) + __.StrM( OstDeb , 17 ) + " " + __.StrM( OstCre , 17 ) + CRLF + SEPARATOR + CRLF );
					}
				}
			}
			TextWriter2.Close();
			TextWriter1.Close();
			Transfers.Close();
		}
		Amounts.Close();
		return	true;
	}//FOLD00

	//  Выписка по Древовидным счетом клиента//fold00
	public	bool	TreeExtract( int Date1 , int Date2 , string ClientCode , string FileName ) {
		int	TotalMain	=	0;
		int	TotalCur	=	0;
		money	TotalMainDeb	=	0;
		money	TotalMainCre	=	0;
		money	TotalCurDeb	=	0;
		money	TotalCurCre	=	0;
		if	( ! Conn1.IsOpen() )
			return	false;
		if	( ClientCode == null )
			return	false;
		else
			if	( ClientCode.Trim() == CAbc.EMPTY )
				return	false;
		Amounts		= new	CRecordSet( Conn1 );

		if	( Amounts.Open( " exec Mega_TreelikeExtract;1  @FromDate="+Date1.ToString()+",@ToDate="+Date2.ToString()+",@ClientCode='"+ClientCode.Trim()+"'" ) ) {
			Transfers		= new	CRecordSet( Conn2 );
			TextWriter1		= new	CTextWriter();
			TextWriter2		= new	CTextWriter();
			TextWriter1.Create( FileName , CAbc.CHARSET_DOS );

			while	( Amounts.Read() )	{
				TotalMain	=	0;
				TotalCur	=	0;
				TotalMainDeb	=	0;
				TotalMainCre	=	0;
				TotalCurDeb	=	0;
				TotalCurCre	=	0;
				MapAmountFields();
				TreePrintHead( Date1 , Date2 );
				bool	IsReval		=	false;
				if	( Transfers.Open(" exec Mega_TreelikeExtract;2  @FromDate="+Date1.ToString()+",@ToDate="+Date2.ToString()+",@Code='"+__.GetCodeByMoniker(Amounts_Code).Trim()+"%'") ) {
					while	( Transfers.Read() ) {
						MapTransferFields();
						if	( Transfers_Tag==CAbc.EMPTY ) {
							TotalMain++ ;
							IsReval			=	true;
							Transfers_CrncyAmount	=	0;
						}
						else {
							TotalCur++ ;
							if	( IsReval ) {
								IsReval	=	false;
								o("---------------------------------------------------- ----------------- ----------------- ----------------- -----------------",CRLF);
								o("Пpоводок ", __.StrI( TotalMain  , 8 ) , __.Replicate(OneSpace,36)  , __.StrM( TotalCurDeb , 17 ) , OneSpace , __.StrM( TotalCurCre , 17 ) , OneSpace , __.StrM( TotalMainDeb , 17 ) , OneSpace , __.StrM( TotalMainCre , 17 ) , CRLF );
							}
						}
						o(CRLF);
						if	( Transfers_Side == 1 ) {
							TotalMainDeb	+=	Transfers_MainAmount;
							TotalCurDeb	+=	Transfers_CrncyAmount;
							o(	__.StrD( Transfers_DayDate , 8 , 8 )	, OneSpace
							,	__.Left( Transfers_DocNum , 10 )		, OneSpace
							,	__.Left( Transfers_TargetCode , 6 )	, OneSpace
							,	__.Left( Transfers_CreditCode , 14 )	, OneSpace
							,	__.Left( Transfers_CreditState , 10 )	, OneSpace
							,	__.StrM( Transfers_CrncyAmount , 17 )	, OneSpace
							,	EmptyM					, OneSpace
							,	__.StrM( Transfers_MainAmount , 17 )	, OneSpace
							,	EmptyM					, CRLF
							);
						}
						else {
							TotalMainCre	+=	Transfers_MainAmount;
							TotalCurCre	+=	Transfers_CrncyAmount;
							o(	__.StrD( Transfers_DayDate , 8 , 8 )	, OneSpace
							,	__.Left( Transfers_DocNum , 10 )		, OneSpace
							,	__.Left( Transfers_SourceCode , 6 )	, OneSpace
							,	__.Left( Transfers_DebetCode , 14 )	, OneSpace
							,	__.Left( Transfers_DebetState , 10 )	, OneSpace
							,	EmptyM					, OneSpace
							,	__.StrM( Transfers_CrncyAmount , 17 )	, OneSpace
							,	EmptyM					, OneSpace
							,	__.StrM( Transfers_MainAmount , 17 )	, CRLF
							);
						}
						TreePrintPurpose();
					}
				}
				TreePrintTotals( Date1 , Date2 , TotalMain , TotalCur , TotalMainDeb , TotalMainCre , TotalCurDeb , TotalCurCre );
			}
			TextWriter1.Close();
			Transfers.Close();
		}
		Amounts.Close();
		return	true;
	}//FOLD00

	void	TreePrintHead( int DateFrom , int DateInto ) {//fold00
		//o( CRLF , "Виконавець" , __.StrI( Amounts_UserId , 5 ) , OneSpace , __.Right( Amounts_UserName , 106) , CRLF ) ;
		o( CRLF ,  __.Left( Bank_Name , 110) , "Банк" , __.Right( Bank_Code , 10 ) );
		int	xPos	=	Amounts_Code.IndexOf(".");
		if	( xPos > 1 ) {	// Это субсчет
			o( CRLF , "ЕДРПО " , __.Right( Amounts_State,10 ) , OneSpace , __.Right( "Рахунок " + Amounts_Code.Substring( 0 , xPos ) + OneSpace + Amounts_Tag , 107 ) );
			o( CRLF , "Назва ", __.Right( Amounts_Name , 118) , CRLF);
			o( CRLF , "Субрахунок ", __.Right( Amounts_Code , 112) , CRLF);
		}
		else {
			o( CRLF , "ЕДРПО " , __.Right( Amounts_State,10 ) , OneSpace , __.Right( "Рахунок " + Amounts_Code + OneSpace  + Amounts_Tag , 107 ) );
			o( CRLF , "Назва ", __.Right( Amounts_Name , 118) , CRLF);
		}
		if	( DateFrom == DateInto )
			o(CRLF , "                                       Виписка/Особовий рахунок за ", __.StrD(DateFrom,8,8) , CRLF , CRLF );
		else
			o(CRLF , "                                   Виписка/Особовий рахунок з ", __.StrD(DateFrom,8,8) ," по " , __.StrD(DateInto,8,8) , CRLF , CRLF );
		o("Дата     Документ   Банк   Рахунок        ЕДРПУ                  Дебет            Кредит             Дебет            Кредит" , CRLF );
		o("-------- ---------- ------ -------------- ---------- ----------------- ----------------- ----------------- -----------------" , CRLF );
	}//FOLD00

	void	TreePrintTotals( int DateFrom , int DateInto , int TotalMain , int TotalCur , money TotalMainDeb , money TotalMainCre , money TotalCurDeb , money TotalCurCre ) {//fold00
		money	OstDeb		=	0;
		money	OstCre		=	Amounts_CrncyAmount ;
		money	InpDeb		=	0 ;
		money	InpCre		=	OstCre - TotalCurCre + TotalCurDeb ;
		money	OstDebEq	=	0 ;
		money	OstCreEq	=	Amounts_MainAmount ;
		money	InpDebEq	=	0 ;
		money	InpCreEq	=	OstCreEq - TotalMainCre + TotalMainDeb ;
		if	( InpCre<0 ) {
			InpDeb		=	0-InpCre ;
			InpCre		=	0 ;
		}
		if	( OstCre<0 ) {
			OstDeb		=	0-OstCre;
			OstCre		=	0;
		}
		if	( InpCreEq<0 ) {
			InpDebEq	=	0-InpCreEq;
			InpCreEq	=	0;
		}
		if	( OstCreEq<0 ) {
			OstDebEq	=	0-OstCreEq;
			OstCreEq	=	0;
		}
		o("==================================================== ================= ================= ================= =================",CRLF);
		o("Пpоводок ", __.StrI( TotalMain +TotalCur , 8 ) , __.Replicate(OneSpace,36) , __.StrM( TotalCurDeb , 17 ) , OneSpace , __.StrM( TotalCurCre , 17 ) , OneSpace , __.StrM( TotalMainDeb , 17 ) , OneSpace , __.StrM( TotalMainCre , 17 ) , CRLF );
		o( CRLF , "Останнiй   " , __.StrD( Amounts_CrncyLastDate,8,8) , "                                  ","----------------- ----------------- ----------------- -----------------");
		o( CRLF , "Вхiдний    " , __.StrD( Amounts_MainLastDate ,8,8) , "                                  ", __.StrM( InpDeb , 17 ) , OneSpace , __.StrM( InpCre , 17 ), OneSpace , __.StrM( InpDebEq , 17 ), OneSpace , __.StrM( InpCreEq , 17 ) );
		o( CRLF , "Вихiдний   " , __.StrD( DateInto, 8,8)             , "                                  ", __.StrM( OstDeb , 17 ) , OneSpace , __.StrM( OstCre , 17 ), OneSpace , __.StrM( OstDebEq , 17 ), OneSpace , __.StrM( OstCreEq , 17 ) , CRLF );
		o( CRLF , "Курс НБУ на " , __.StrD(Amounts_MainLastDate,8,8)," =", StrF( Amounts_RateBefore / 100 ) ," за " , ( Amounts_Pieces / 100 ).ToString() , OneSpace , Amounts_Tag );
		o( CRLF , "Курс НБУ на " , __.StrD(DateInto,8,8)," =" , StrF( Amounts_RateAfter / 100 ) ," за " , ( Amounts_Pieces / 100 ).ToString() , OneSpace , Amounts_Tag , CRLF );
		o( CRLF , __.Replicate("- ",40) , CRLF );
	}//FOLD00

	void	TreePrintPurpose() {//fold00
		o(         "Призначення:" ,	__.SubStr( Transfers_Purpose,0,39 ) , CRLF );
			if	( Transfers_Purpose.Length > 40 )
				o( __.SubStr( Transfers_Purpose,40,91 ) , CRLF );
			if	( Transfers_Purpose.Length > 92 )
				o( __.SubStr( Transfers_Purpose,92,143 ) , CRLF );
			if	( Transfers_Purpose.Length > 144 )
				o( __.SubStr( Transfers_Purpose,144,195 ) , CRLF );
			if	( Transfers_Purpose.Length > 196 )
				o( __.SubStr( Transfers_Purpose,197,239 ) , CRLF );
			if	( Transfers_Side == 1 ) {
				o( "Банк:" ,	__.SubStr( Transfers_TargetName , 0 , 44 ) , CRLF );
				o( "Крсп:" ,	__.SubStr( Transfers_CreditName , 0 , 46 ) , CRLF ) ;
				if	( Transfers_CreditName.Length > 47 )
					o( __.SubStr( Transfers_CreditName,47,79 ) , CRLF ) ;
			}
			else	{
				o( "Банк:" ,	__.SubStr( Transfers_SourceName , 0 , 44 ) , CRLF );
	       			o( "Крсп:" ,	__.SubStr( Transfers_DebetName , 0 , 46 ) , CRLF ) ;
				if	( Transfers_DebetName.Length > 47 )
					o( __.SubStr( Transfers_DebetName,47,79 ) , CRLF ) ;
			}
	}//FOLD00

	public	bool	Balance( string	FileName , int	FromDate , int ToDate ,	bool WithRegions ) {//fold00
		bool	WasSeparator	=	false
		,	NeedTotalGroup	=	false
		,	NeedTotalTopic	=	false
		;
		int	I		=	0
		,	Topic		=	0
		,	Class		=	0
		,	Group		=	0
		,	Region		=	0
		,	Account		=	0
		,	OldClass	=	0
		,	OldTopic	=	0
		,	OldGroup	=	0
		,	OldAccount	=	0
		;

		string	Name		=	CAbc.EMPTY
		,	OldName		=	CAbc.EMPTY
		,	PIPE		=	"|"
		,	EMPTY_REGION	=	"    |"
		,	CMD_TEXT1	=	( WithRegions )
					?	" exec dbo.Mega_Report_Balance;3  " + FromDate.ToString() + " , " + ToDate.ToString()
					:	" exec dbo.Mega_Report_Balance;1  " + FromDate.ToString() + " , " + ToDate.ToString()
		,	CMD_TEXT2	=	( WithRegions )
					?	" exec dbo.Mega_Report_Balance;4  " + FromDate.ToString() + " , " + ToDate.ToString()
					:	" exec dbo.Mega_Report_Balance;2  " + FromDate.ToString() + " , " + ToDate.ToString()
		,	SEPARATOR	=	( WithRegions )
					?	"-------+----+----------------------------------------+-----------------+-----------------+-----------------+-----------------+"  + CAbc.CRLF
					:	"-------+----------------------------------------+-----------------+-----------------+-----------------+-----------------+"  + CAbc.CRLF
		,	HEADER		=	( WithRegions )
					?	"       |    |                                        |              Обоpоти              |              Залишки              |"  + CAbc.CRLF
					+	"       |    |                                        +-----------------+-----------------+-----------------+-----------------+"  + CAbc.CRLF
					+	"Рахунок|Обл.|          Найменування pахунку          |      Дебет      |      Кpедит     |      Актив      |      Пасив      |" + CAbc.CRLF
					:	"       |                                        |              Обоpоти              |              Залишки              |" + CAbc.CRLF
					+	"       |                                        +-----------------+-----------------+-----------------+-----------------+" + CAbc.CRLF
					+	"Рахунок|          Найменування pахунку          |      Дебет      |      Кpедит     |      Актив      |      Пасив      |" + CAbc.CRLF
		;
		string[] TopicName	= new	string[1000]
		;
		if	( ! Conn1.IsOpen() )
			return	false;
		CRecordSet	RecordSet	= new	CRecordSet( Conn1 );
		for	( I = 1 ; I < 1000 ; I ++ )
			TopicName[I] = "";
		if	( RecordSet.Open( "select Id , Name from dbo.SV_Topics with ( NoLock ) where Id<1000 " ) ) {
			int	Id	=	0;
			while	( RecordSet.Read() ) {
				Id		=	__.CInt(	RecordSet[ "Id" ] );
				TopicName[ Id ]	=	__.FixUkrI(	RecordSet[ "Name" ] );
			}
		}
		money	Debit		=	0
		,	Credit		=	0
		,	Active		=	0
		,	Passive		=	0
		,	TotalDAcc	=	0
		,	TotalCAcc	=	0
		,	TotalAAcc	=	0
		,	TotalPAcc	=	0
		,	TotalDItog	=	0
		,	TotalCItog	=	0
		,	TotalAItog	=	0
		,	TotalPItog	=	0
		,	TotalDClass	=	0
		,	TotalCClass	=	0
		,	TotalAClass	=	0
		,	TotalPClass	=	0
		,	TotalDTopic	=	0
		,	TotalCTopic	=	0
		,	TotalATopic	=	0
		,	TotalPTopic	=	0
		,	TotalDGroup	=	0
		,	TotalCGroup	=	0
		,	TotalAGroup	=	0
		,	TotalPGroup	=	0
		,	TotalDResult	=	0
		,	TotalCResult	=	0
		,	TotalAResult	=	0
		,	TotalPResult	=	0
		;
		if	( RecordSet.Open( CMD_TEXT1 ) ) {
			TextWriter1	= new	CTextWriter();
			if	( ! TextWriter1.Create( FileName , CAbc.CHARSET_DOS ) ) {
				RecordSet.Close();
				return	false;
			}
			o( CAbc.CRLF , __.Space( 19 ) , __.Center( Bank_Name ,80 ) );
			o( CAbc.CRLF , __.Space( 19 ) , __.Center( "ОБОРОТНО-САЛЬДОВИЙ БАЛАНС" , 80 ) );
			if	( FromDate == ToDate )
				o( CAbc.CRLF , __.Space( 19 ) , __.Center( "ЗА " + __.StrD( FromDate , 10, 10 ) , 80 ) , CAbc.CRLF , CAbc.CRLF );
			else
				o( CAbc.CRLF , __.Space( 19 ) , __.Center( "З " + __.StrD( FromDate , 10, 10 ) +" ПО " + __.StrD( ToDate , 10 , 10 ) , 80 ) , CAbc.CRLF , CAbc.CRLF );

			o( SEPARATOR , HEADER , SEPARATOR );
			while	( RecordSet.Read() )	{
				Name		=	__.FixUkrI(	RecordSet[ "Name" ] );
				Debit		=	__.CCur(	RecordSet[ "Debit" ] );
				Credit		=	__.CCur(	RecordSet[ "Credit"] );
				Active		=	__.CCur(	RecordSet[ "Active"] );
				Passive		=	__.CCur(	RecordSet[ "Passive"] );
				Account		=	__.CInt(	RecordSet[ "Topic" ] );
				Group		=	__.CInt(		( Account.ToString("0000")).Substring(0,3) );;
				Topic		=	__.CInt(		( Account.ToString("0000")).Substring(0,2) );
				Class		=	__.CInt(		( Account.ToString("0000")).Substring(0,1) );
				if	( WithRegions )
					Region	=	__.CInt(	RecordSet[ "Region" ] );
				if	( WithRegions && ( Account != OldAccount ) && ( OldAccount != 0 ) && ( (TotalDAcc!=0) || (TotalCAcc!=0) || (TotalAAcc!=0) || (TotalPAcc)!=0) ) {
					if	( OldGroup == 0 )
						OldGroup=Group;
                  			o(	" " , __.StrI( OldAccount , 4 )  , "  |"
					,	(	( WithRegions )
						?	EMPTY_REGION
						:	CAbc.EMPTY
						)
					,	__.Left( OldName , 40 )
					,	PIPE , __.StrM( TotalDAcc , 17 )
					,	PIPE , __.StrM( TotalCAcc , 17 )
					,	PIPE , __.StrM( TotalAAcc , 17 )
					,	PIPE , __.StrM( TotalPAcc , 17 )
					,	PIPE , CAbc.CRLF
					);
					TotalDAcc=0;TotalCAcc=0;TotalAAcc=0;TotalPAcc=0;
				}
				if	( Group != OldGroup )	{
                                        if	( OldGroup != 0 ) {
                     				if	( NeedTotalGroup ) {
                            				o( SEPARATOR );
							NeedTotalTopic=true;
						}
	                  			o(	" " , __.StrI( OldGroup , 3 )  , "   |"
						,	(	( WithRegions )
							?	EMPTY_REGION
							:	CAbc.EMPTY
							)
						,	__.Left( TopicName[ OldGroup ], 40 )
						,	PIPE , __.StrM( TotalDGroup , 17 )
						,	PIPE , __.StrM( TotalCGroup , 17 )
						,	PIPE , __.StrM( TotalAGroup , 17 )
						,	PIPE , __.StrM( TotalPGroup , 17 )
						,	PIPE , CAbc.CRLF , SEPARATOR
						);
						WasSeparator=true;
					}
					if	( Topic != OldTopic ) {
						if	( OldTopic != 0 ) {
							if	( NeedTotalTopic ) {
								if	(  ! WasSeparator )
									o( SEPARATOR );
			                  			o(	" " , __.StrI( OldTopic , 2 )  , "    |"
								,	(	( WithRegions )
									?	EMPTY_REGION
									:	CAbc.EMPTY
									)
								,	__.Left( TopicName[ OldTopic ], 40 )
								,	PIPE , __.StrM( TotalDTopic , 17 )
								,	PIPE , __.StrM( TotalCTopic , 17 )
								,	PIPE , __.StrM( TotalATopic , 17 )
								,	PIPE , __.StrM( TotalPTopic , 17 )
								,	PIPE , CAbc.CRLF , SEPARATOR
								);
								WasSeparator=true;
							}
							if	( ( Class != OldClass) && (OldClass!=0) ) {
								if	( OldClass != 5 ) {
									if	( ! WasSeparator )
										o( SEPARATOR );
				                  			o(	" " , __.StrI( OldClass , 1 )  , "     |"
									,	(	( WithRegions )
										?	EMPTY_REGION
										:	CAbc.EMPTY
										)
									,	__.Left( TopicName[ OldClass ], 40 )
									,	PIPE , __.StrM( TotalDClass , 17 )
									,	PIPE , __.StrM( TotalCClass , 17 )
									,	PIPE , __.StrM( TotalAClass , 17 )
									,	PIPE , __.StrM( TotalPClass , 17 )
									,	PIPE , CAbc.CRLF , SEPARATOR
									);
									WasSeparator=false;
								}
								if	( (Class>5) && (OldClass<6) ) {
									if	( ! WasSeparator )
										o( SEPARATOR );
									CRecordSet	YearResult	= new	CRecordSet( Conn2 );
									if	( YearResult.Open( CMD_TEXT2 ) ) {
										while	( YearResult.Read() ) {
											TotalDResult	+=	__.CCur( YearResult["Debit"] );
											TotalCResult	+=	__.CCur( YearResult["Credit"] );
											TotalAResult	+=	__.CCur( YearResult["Active"] );
											TotalPResult	+=	__.CCur( YearResult["Passive"] );
											if	( WithRegions )
							                  			o(	" " , __.Left( YearResult["Topic"] , 4 )  , "  |"
												,	(	( WithRegions )
													?	__.Right( YearResult["Region"] , 3 ) + " |"
													:	CAbc.EMPTY
													)
												,	__.Left( YearResult["Name"] , 40 )
												,	PIPE , __.StrM( __.CCur( YearResult["Debit"] ) , 17 )
												,	PIPE , __.StrM( __.CCur( YearResult["Credit"] ) , 17 )
												,	PIPE , __.StrM( __.CCur( YearResult["Active"] ) , 17 )
												,	PIPE , __.StrM( __.CCur( YearResult["Passive"] ) , 17 )
												,	PIPE , CAbc.CRLF
												);
										}
					                  			o(	" 5999  |"
										,	(	( WithRegions )
											?	EMPTY_REGION
											:	CAbc.EMPTY
											)
										,	__.Left( "Фiнансовий результат " , 40 )
										,	PIPE , __.StrM( TotalDResult , 17 )
										,	PIPE , __.StrM( TotalCResult , 17 )
										,	PIPE , __.StrM( TotalAResult , 17 )
										,	PIPE , __.StrM( TotalPResult , 17 )
										,	PIPE , CAbc.CRLF , SEPARATOR
										);
									}
									YearResult.Close();
					                                WasSeparator	=	false;
									TotalDClass	+=	TotalDResult;
									TotalCClass	+=	TotalCResult;
									TotalAClass	+=	TotalAResult;
									TotalPClass	+=	TotalPResult;
									TotalDItog	+=	TotalDResult;
									TotalCItog	+=	TotalCResult;
									TotalAItog	+=	TotalAResult;
									TotalPItog	+=	TotalPResult;
								}
								if	( OldClass==5 ) {
					                                 if	( ! WasSeparator )
									 	o( SEPARATOR );
				                  			o(	" " , __.StrI( OldClass , 1 )  , "     |"
									,	(	( WithRegions )
										?	EMPTY_REGION
										:	CAbc.EMPTY
										)
									,	__.Left( TopicName[ OldClass ], 40 )
									,	PIPE , __.StrM( TotalDClass , 17 )
									,	PIPE , __.StrM( TotalCClass , 17 )
									,	PIPE , __.StrM( TotalAClass , 17 )
									,	PIPE , __.StrM( TotalPClass , 17 )
									,	PIPE , CAbc.CRLF
									);
                              					}
								if	( (Class>5) && (OldClass<6) ) {
				                  			o(	SEPARATOR , SEPARATOR ,	"       |"
									,	(	( WithRegions )
										?	EMPTY_REGION
										:	CAbc.EMPTY
										)
									,	__.Left( "  В С Ь О Г О   З А   Б А Л А Н С О М  " , 40 )
									,	PIPE , __.StrM( TotalDItog , 17 )
									,	PIPE , __.StrM( TotalCItog , 17 )
									,	PIPE , __.StrM( TotalAItog , 17 )
									,	PIPE , __.StrM( TotalPItog , 17 )
									,	PIPE , CAbc.CRLF , SEPARATOR
									);
									WasSeparator=true;
								}
								o( SEPARATOR );
								TotalDClass=0;TotalCClass=0;TotalAClass=0;TotalPClass=0;
			   				}
						}
						NeedTotalTopic=false;TotalDTopic=0;TotalCTopic=0;TotalATopic=0;TotalPTopic=0;
					}
					NeedTotalGroup=false;TotalDGroup=0;TotalCGroup=0;TotalAGroup=0;TotalPGroup=0;
				}
				OldName		=	Name;
				OldClass	=	Class;
				OldTopic	=	Topic;
				OldGroup	=	Group;
				OldAccount	=	Account;
				TotalDAcc	+=	Debit;
				TotalCAcc	+=	Credit;
				TotalAAcc	+=	Active;
				TotalPAcc	+=	Passive;
				TotalDGroup	+=	Debit;
				TotalCGroup	+=	Credit;
				TotalAGroup	+=	Active;
				TotalPGroup	+=	Passive;
				TotalDTopic	+=	Debit;
				TotalCTopic	+=	Credit;
				TotalATopic	+=	Active;
				TotalPTopic	+=	Passive;
				TotalDClass	+=	Debit;
				TotalCClass	+=	Credit;
				TotalAClass	+=	Active;
				TotalPClass	+=	Passive;
				TotalDItog	+=	Debit;
				TotalCItog	+=	Credit;
				TotalAItog	+=	Active;
				TotalPItog	+=	Passive;
				if	( Debit!=0 || Credit!=0  || Active!=0 || Passive!=0 ) {
                  			o(	" " , __.StrI( Account , 4 )  , "  |"
					,	(	( WithRegions )
						?	__.StrI( Region , 3 ) + " |"
						:	CAbc.EMPTY
						)
					,	(	( WithRegions )
						?	__.Space( 40 )
						:	__.Left( Name , 40 )
						)
					,	PIPE , __.StrM( Debit , 17 )
					,	PIPE , __.StrM( Credit , 17 )
					,	PIPE , __.StrM( Active , 17 )
					,	PIPE , __.StrM( Passive , 17 )
					,	PIPE , CAbc.CRLF
					);
					WasSeparator	=	false;
					NeedTotalGroup	=	true;
				}
                        }
			if	( OldAccount != 0 ) {
				if	( WithRegions && ( (TotalDAcc!=0) || (TotalCAcc!=0) || (TotalAAcc!=0) || (TotalPAcc)!=0) )
                  			o(	" " , __.StrI( OldAccount , 4 )  , "  |"
					,	(	( WithRegions )
						?	EMPTY_REGION
						:	CAbc.EMPTY
						)
					,	__.Left( OldName , 40 )
					,	PIPE , __.StrM( TotalDAcc , 17 )
					,	PIPE , __.StrM( TotalCAcc , 17 )
					,	PIPE , __.StrM( TotalAAcc , 17 )
					,	PIPE , __.StrM( TotalPAcc , 17 )
					,	PIPE , CAbc.CRLF , SEPARATOR
					);
               			o(	" " , __.StrI( OldGroup , 3 )  , "   |"
				,	(	( WithRegions )
					?	EMPTY_REGION
					:	CAbc.EMPTY
					)
				,	__.Left( TopicName[ OldGroup ], 40 )
				,	PIPE , __.StrM( TotalDGroup , 17 )
				,	PIPE , __.StrM( TotalCGroup , 17 )
				,	PIPE , __.StrM( TotalAGroup , 17 )
				,	PIPE , __.StrM( TotalPGroup , 17 )
				,	PIPE , CAbc.CRLF , SEPARATOR
				);
               			o(	" " , __.StrI( OldTopic , 2 )  , "    |"
					,	(	( WithRegions )
					?	EMPTY_REGION
					:	CAbc.EMPTY
					)
				,	__.Left( TopicName[ OldTopic ], 40 )
				,	PIPE , __.StrM( TotalDTopic , 17 )
				,	PIPE , __.StrM( TotalCTopic , 17 )
				,	PIPE , __.StrM( TotalATopic , 17 )
				,	PIPE , __.StrM( TotalPTopic , 17 )
				,	PIPE , CAbc.CRLF , SEPARATOR
				);
               			o(	" " , __.StrI( OldClass , 1 )  , "     |"
					,	(	( WithRegions )
					?	EMPTY_REGION
					:	CAbc.EMPTY
					)
				,	__.Left( TopicName[ OldClass ], 40 )
				,	PIPE , __.StrM( TotalDClass , 17 )
				,	PIPE , __.StrM( TotalCClass , 17 )
				,	PIPE , __.StrM( TotalAClass , 17 )
				,	PIPE , __.StrM( TotalPClass , 17 )
				,	PIPE , CAbc.CRLF
				);
			}
			o( SEPARATOR , CAbc.CRLF , CAbc.CRLF  );
			if	( Branch_Kind==0 )
				o( "   Голова Правлiння    ___________________ " , Chief_Name , CAbc.CRLF , CAbc.CRLF );
			else
				o( "   Диpектоp фiлii      ___________________ " , Chief_Name , CAbc.CRLF , CAbc.CRLF );
			o("   Головний бухгалтеp ______________ " , ChiefAcc_Name , CAbc.CRLF , CAbc.CRLF );
			o( __.Right( "--- Розраховано " + __.Now().ToString() + " ---" , 120 ) , CAbc.CRLF ,  __.Replicate("-", 120 ) , CAbc.CRLF );
         		TextWriter1.Close();
		}
		else {
			RecordSet.Close();
			return	false;
		}
		RecordSet.Close();
		return	true;
	}//FOLD00

	public	bool	ConsolidatedBalance( string	FileName , int	FromDate , int ToDate ) {//FOLD00
		byte	SavedColor	=	0
		;
		bool	HaveError	=	false
		,	WasSeparator	=	false
		,	NeedTotalGroup	=	false
		,	NeedTotalTopic	=	false
		;
		int	I		=	0
		,	Topic		=	0
		,	Class		=	0
		,	Group		=	0
		,	Region		=	0
		,	Account		=	0
		,	OldClass	=	0
		,	OldTopic	=	0
		,	OldGroup	=	0
		,	OldAccount	=	0
		;
		string	Name		=	CAbc.EMPTY
		,	OldName		=	CAbc.EMPTY
		,	PIPE		=	"|"
		,	EMPTY_REGION	=	"        |"
		,	CMD_TEXT0	=	" exec dbo.Mega_Check_Balance;2  @FromDate = " + FromDate.ToString() + " , @ToDate = " + ToDate.ToString()
		,	CMD_TEXT1	=	" exec dbo.Mega_Report_PlainBalance;1  @NoTotal = 1 , @FromDate = " + FromDate.ToString() + " , @ToDate = " + ToDate.ToString()
		,	CMD_TEXT2	=	" exec dbo.Mega_Report_PlainBalance;2  @FromDate = " + FromDate.ToString() + " , @ToDate = " + ToDate.ToString()
		,	SEPARATOR	=	"-------+--------+----------------------------------------+-----------------+-----------------+-----------------+-----------------+"  + CAbc.CRLF
		,	HEADER		=	"       |        |                                        |              Обоpоти              |              Залишки              |"  + CAbc.CRLF
					+	"       |        |                                        +-----------------+-----------------+-----------------+-----------------+"  + CAbc.CRLF
					+	"Рахунок|  Банк  |          Найменування pахунку          |      Дебет      |      Кpедит     |      Актив      |      Пасив      |" + CAbc.CRLF
		;
		string[] TopicName	= new	string[1000]
		;
		if	( ! Conn1.IsOpen() )
			return	false;
		CRecordSet	RecordSet	= new	CRecordSet( Conn1 );
		for	( I = 1 ; I < 1000 ; I ++ )
			TopicName[I] = "";
		if	( RecordSet.Open( "select Id , Name from dbo.SV_Topics with ( NoLock ) where Id<1000 " ) ) {
			int	Id	=	0;
			while	( RecordSet.Read() ) {
				Id		=	__.CInt(	RecordSet[ "Id" ] );
				TopicName[ Id ]	=	__.FixUkrI(	RecordSet[ "Name" ] );
			}
		}
		money	Debit		=	0
		,	Credit		=	0
		,	Active		=	0
		,	Passive		=	0
		,	TotalDAcc	=	0
		,	TotalCAcc	=	0
		,	TotalAAcc	=	0
		,	TotalPAcc	=	0
		,	TotalDItog	=	0
		,	TotalCItog	=	0
		,	TotalAItog	=	0
		,	TotalPItog	=	0
		,	TotalDClass	=	0
		,	TotalCClass	=	0
		,	TotalAClass	=	0
		,	TotalPClass	=	0
		,	TotalDTopic	=	0
		,	TotalCTopic	=	0
		,	TotalATopic	=	0
		,	TotalPTopic	=	0
		,	TotalDGroup	=	0
		,	TotalCGroup	=	0
		,	TotalAGroup	=	0
		,	TotalPGroup	=	0
		,	TotalDResult	=	0
		,	TotalCResult	=	0
		,	TotalAResult	=	0
		,	TotalPResult	=	0
		;
		if	( RecordSet.Open( CMD_TEXT1 ) ) {
			TextWriter1	= new	CTextWriter();
			if	( ! TextWriter1.Create( FileName , CAbc.CHARSET_DOS ) ) {
				RecordSet.Close();
				return	false;
			}
			CRecordSet	Check	= new	CRecordSet( Conn2 );
			if	( Check.Open( CMD_TEXT0 ) )
				if	( Check.Read() ) {
					HaveError	=	true;
					CConsole.Clear();
					CCommon.Print( CAbc.EMPTY );
					o( CAbc.CRLF , SEPARATOR , CAbc.CRLF , "  ПОМИЛКИ, ЩО ЗНАЙДЕНО ПРИ ПЕРЕВIРЦI ЗВЕДЕНОГО БАЛАНСУ :"  , CAbc.CRLF);
					do {
						CCommon.Print( Check["Result"] );
						o( CAbc.CRLF , Check["Result"] , CAbc.CRLF );
					} while	( Check.Read() );
					o( SEPARATOR , CAbc.CRLF );
				}
			Check.Close();
			o( CAbc.CRLF , __.Space( 19 ) , __.Center( Bank_Name ,80 ) );
			o( CAbc.CRLF , __.Space( 9 ) , __.Center( "ЗВЕДЕНИЙ ОБОРОТНО-САЛЬДОВИЙ БАЛАНС" , 100 ) );
			if	( FromDate == ToDate )
				o( CAbc.CRLF , __.Space( 19 ) , __.Center( "ЗА " + __.StrD( FromDate , 10, 10 ) , 80 ) , CAbc.CRLF , CAbc.CRLF );
			else
				o( CAbc.CRLF , __.Space( 19 ) , __.Center( "З " + __.StrD( FromDate , 10, 10 ) +" ПО " + __.StrD( ToDate , 10 , 10 ) , 80 ) , CAbc.CRLF , CAbc.CRLF );

			o( SEPARATOR , HEADER , SEPARATOR );
			while	( RecordSet.Read() )	{
				Account		=	__.CInt(	RecordSet[ "Topic" ] );
				Region		=	__.CInt(	RecordSet[ "Bank"  ] );
				Name		=	__.FixUkrI(	RecordSet[ "Name"  ] );
				Debit		=	__.CCur(	RecordSet[ "Debit" ] );
				Credit		=	__.CCur(	RecordSet[ "Credit"] );
				Active		=	__.CCur(	RecordSet[ "Active"] );
				Passive		=	__.CCur(	RecordSet[ "Passive"] );
				Group		=	__.CInt(		( Account.ToString("0000")).Substring(0,3) );;
				Topic		=	__.CInt(		( Account.ToString("0000")).Substring(0,2) );
				Class		=	__.CInt(		( Account.ToString("0000")).Substring(0,1) );
				if	( ( Account != OldAccount ) && ( OldAccount != 0 ) && ( (TotalDAcc!=0) || (TotalCAcc!=0) || (TotalAAcc!=0) || (TotalPAcc)!=0) ) {
					if	( OldGroup == 0 )
						OldGroup=Group;
                  			o(	" " , __.StrI( OldAccount , 4 )  , "  |"
					,	EMPTY_REGION
					,	__.Left( OldName , 40 )
					,	PIPE , __.StrM( TotalDAcc , 17 )
					,	PIPE , __.StrM( TotalCAcc , 17 )
					,	PIPE , __.StrM( TotalAAcc , 17 )
					,	PIPE , __.StrM( TotalPAcc , 17 )
					,	PIPE , CAbc.CRLF
					);
					TotalDAcc=0;TotalCAcc=0;TotalAAcc=0;TotalPAcc=0;
				}
				if	( Group != OldGroup )	{
                                        if	( OldGroup != 0 ) {
                     				if	( NeedTotalGroup ) {
                            				o( SEPARATOR );
							NeedTotalTopic=true;
						}
	                  			o(	" " , __.StrI( OldGroup , 3 )  , "   |"
						,	EMPTY_REGION
						,	__.Left( TopicName[ OldGroup ], 40 )
						,	PIPE , __.StrM( TotalDGroup , 17 )
						,	PIPE , __.StrM( TotalCGroup , 17 )
						,	PIPE , __.StrM( TotalAGroup , 17 )
						,	PIPE , __.StrM( TotalPGroup , 17 )
						,	PIPE , CAbc.CRLF , SEPARATOR
						);
						WasSeparator=true;
					}
					if	( Topic != OldTopic ) {
						if	( OldTopic != 0 ) {
							if	( NeedTotalTopic ) {
								if	(  ! WasSeparator )
									o( SEPARATOR );
			                  			o(	" " , __.StrI( OldTopic , 2 )  , "    |"
								,	EMPTY_REGION
								,	__.Left( TopicName[ OldTopic ], 40 )
								,	PIPE , __.StrM( TotalDTopic , 17 )
								,	PIPE , __.StrM( TotalCTopic , 17 )
								,	PIPE , __.StrM( TotalATopic , 17 )
								,	PIPE , __.StrM( TotalPTopic , 17 )
								,	PIPE , CAbc.CRLF , SEPARATOR
								);
								WasSeparator=true;
							}
							if	( ( Class != OldClass) && (OldClass!=0) ) {
								if	( OldClass != 5 ) {
									if	( ! WasSeparator )
										o( SEPARATOR );
				                  			o(	" " , __.StrI( OldClass , 1 )  , "     |"
									,	EMPTY_REGION
									,	__.Left( TopicName[ OldClass ], 40 )
									,	PIPE , __.StrM( TotalDClass , 17 )
									,	PIPE , __.StrM( TotalCClass , 17 )
									,	PIPE , __.StrM( TotalAClass , 17 )
									,	PIPE , __.StrM( TotalPClass , 17 )
									,	PIPE , CAbc.CRLF , SEPARATOR
									);
									WasSeparator=false;
								}
								/*
								if	( (Class>5) && (OldClass<6) ) {
									if	( ! WasSeparator )
										o( SEPARATOR );
									CRecordSet	YearResult	= new	CRecordSet( Conn2 );
									if	( YearResult.Open( CMD_TEXT2 ) ) {
										while	( YearResult.Read() ) {
											TotalDResult	+=	__.CCur( YearResult["Debit"] );
											TotalCResult	+=	__.CCur( YearResult["Credit"] );
											TotalAResult	+=	__.CCur( YearResult["Active"] );
											TotalPResult	+=	__.CCur( YearResult["Passive"] );
							                  		o(	" " , __.Left( YearResult["Topic"] , 4 )  , "  |"
											,	" " , __.Right( YearResult["Bank"] , 6 ) , " |"
											,	__.Left( YearResult["Name"] , 40 )
											,	PIPE , __.StrM( __.CCur( YearResult["Debit"] ) , 17 )
											,	PIPE , __.StrM( __.CCur( YearResult["Credit"] ) , 17 )
											,	PIPE , __.StrM( __.CCur( YearResult["Active"] ) , 17 )
											,	PIPE , __.StrM( __.CCur( YearResult["Passive"] ) , 17 )
											,	PIPE , CAbc.CRLF
											);
										}
					                  			o(	" 5999  |"
										,	EMPTY_REGION
										,	__.Left( "Фiнансовий результат " , 40 )
										,	PIPE , __.StrM( TotalDResult , 17 )
										,	PIPE , __.StrM( TotalCResult , 17 )
										,	PIPE , __.StrM( TotalAResult , 17 )
										,	PIPE , __.StrM( TotalPResult , 17 )
										,	PIPE , CAbc.CRLF , SEPARATOR
										);
									}
									YearResult.Close();
					                                WasSeparator	=	false;
									TotalDClass	+=	TotalDResult;
									TotalCClass	+=	TotalCResult;
									TotalAClass	+=	TotalAResult;
									TotalPClass	+=	TotalPResult;
									TotalDItog	+=	TotalDResult;
									TotalCItog	+=	TotalCResult;
									TotalAItog	+=	TotalAResult;
									TotalPItog	+=	TotalPResult;
								}*/
								if	( OldClass==5 ) {
					                                 if	( ! WasSeparator )
									 	o( SEPARATOR );
				                  			o(	" " , __.StrI( OldClass , 1 )  , "     |"
									,	EMPTY_REGION
									,	__.Left( TopicName[ OldClass ], 40 )
									,	PIPE , __.StrM( TotalDClass , 17 )
									,	PIPE , __.StrM( TotalCClass , 17 )
									,	PIPE , __.StrM( TotalAClass , 17 )
									,	PIPE , __.StrM( TotalPClass , 17 )
									,	PIPE , CAbc.CRLF
									);
                              					}
								if	( (Class>5) && (OldClass<6) ) {
				                  			o(	SEPARATOR , SEPARATOR ,	"       |"
									,	EMPTY_REGION
									,	__.Left( "  В С Ь О Г О   З А   Б А Л А Н С О М  " , 40 )
									,	PIPE , __.StrM( TotalDItog , 17 )
									,	PIPE , __.StrM( TotalCItog , 17 )
									,	PIPE , __.StrM( TotalAItog , 17 )
									,	PIPE , __.StrM( TotalPItog , 17 )
									,	PIPE , CAbc.CRLF , SEPARATOR
									);
									WasSeparator=true;
								}
								o( SEPARATOR );
								TotalDClass=0;TotalCClass=0;TotalAClass=0;TotalPClass=0;
			   				}
						}
						NeedTotalTopic=false;TotalDTopic=0;TotalCTopic=0;TotalATopic=0;TotalPTopic=0;
					}
					NeedTotalGroup=false;TotalDGroup=0;TotalCGroup=0;TotalAGroup=0;TotalPGroup=0;
				}
				OldName		=	Name;
				OldClass	=	Class;
				OldTopic	=	Topic;
				OldGroup	=	Group;
				OldAccount	=	Account;
				TotalDAcc	+=	Debit;
				TotalCAcc	+=	Credit;
				TotalAAcc	+=	Active;
				TotalPAcc	+=	Passive;
				TotalDGroup	+=	Debit;
				TotalCGroup	+=	Credit;
				TotalAGroup	+=	Active;
				TotalPGroup	+=	Passive;
				TotalDTopic	+=	Debit;
				TotalCTopic	+=	Credit;
				TotalATopic	+=	Active;
				TotalPTopic	+=	Passive;
				TotalDClass	+=	Debit;
				TotalCClass	+=	Credit;
				TotalAClass	+=	Active;
				TotalPClass	+=	Passive;
				TotalDItog	+=	Debit;
				TotalCItog	+=	Credit;
				TotalAItog	+=	Active;
				TotalPItog	+=	Passive;
				if	( Debit!=0 || Credit!=0  || Active!=0 || Passive!=0 ) {
                  			o(	" " , __.StrI( Account , 4 )  , "  |"
					,	" " , __.StrI( Region , 6 ) + " |"
					,	__.Space( 40 )
					,	PIPE , __.StrM( Debit , 17 )
					,	PIPE , __.StrM( Credit , 17 )
					,	PIPE , __.StrM( Active , 17 )
					,	PIPE , __.StrM( Passive , 17 )
					,	PIPE , CAbc.CRLF
					);
					WasSeparator	=	false;
					NeedTotalGroup	=	true;
				}
                        }
			if	( OldAccount != 0 ) {
				if	( ( (TotalDAcc!=0) || (TotalCAcc!=0) || (TotalAAcc!=0) || (TotalPAcc)!=0) )
                  			o(	" " , __.StrI( OldAccount , 4 )  , "  |"
					,	EMPTY_REGION
					,	__.Left( OldName , 40 )
					,	PIPE , __.StrM( TotalDAcc , 17 )
					,	PIPE , __.StrM( TotalCAcc , 17 )
					,	PIPE , __.StrM( TotalAAcc , 17 )
					,	PIPE , __.StrM( TotalPAcc , 17 )
					,	PIPE , CAbc.CRLF , SEPARATOR
					);
               			o(	" " , __.StrI( OldGroup , 3 )  , "   |"
				,	EMPTY_REGION
				,	__.Left( TopicName[ OldGroup ], 40 )
				,	PIPE , __.StrM( TotalDGroup , 17 )
				,	PIPE , __.StrM( TotalCGroup , 17 )
				,	PIPE , __.StrM( TotalAGroup , 17 )
				,	PIPE , __.StrM( TotalPGroup , 17 )
				,	PIPE , CAbc.CRLF , SEPARATOR
				);
               			o(	" " , __.StrI( OldTopic , 2 )  , "    |"
				,	EMPTY_REGION
				,	__.Left( TopicName[ OldTopic ], 40 )
				,	PIPE , __.StrM( TotalDTopic , 17 )
				,	PIPE , __.StrM( TotalCTopic , 17 )
				,	PIPE , __.StrM( TotalATopic , 17 )
				,	PIPE , __.StrM( TotalPTopic , 17 )
				,	PIPE , CAbc.CRLF , SEPARATOR
				);
               			o(	" " , __.StrI( OldClass , 1 )  , "     |"
				,	EMPTY_REGION
				,	__.Left( TopicName[ OldClass ], 40 )
				,	PIPE , __.StrM( TotalDClass , 17 )
				,	PIPE , __.StrM( TotalCClass , 17 )
				,	PIPE , __.StrM( TotalAClass , 17 )
				,	PIPE , __.StrM( TotalPClass , 17 )
				,	PIPE , CAbc.CRLF
				);
			}
			o( SEPARATOR , CAbc.CRLF , CAbc.CRLF  );
			if	( Branch_Kind==0 )
				o( "   Голова Правлiння    ___________________ " , Chief_Name , CAbc.CRLF , CAbc.CRLF );
			else
				o( "   Диpектоp фiлii      ___________________ " , Chief_Name , CAbc.CRLF , CAbc.CRLF );
			o("   Головний бухгалтеp ______________ " , ChiefAcc_Name , CAbc.CRLF , CAbc.CRLF );
			o( __.Right( "--- Розраховано " + __.Now().ToString() + " ---" , 120 ) , CAbc.CRLF ,  __.Replicate("-", 120 ) , CAbc.CRLF );
         		TextWriter1.Close();
		}
		else {
			RecordSet.Close();
			return	false;
		}
		RecordSet.Close();
		if	( HaveError ) {
			SavedColor		=	CConsole.BoxColor ;
			CConsole.BoxColor	=	CConsole.RED*16 + CConsole.WHITE	;
			CConsole.GetBoxChoice(	" При проверке баланса обнаружены ошибки !"
					,	"_________________________________________"
					,	" Для продллжения нажмите ENTER ... "
				) ;
			CConsole.BoxColor	=	SavedColor ;
		}
		return	true;
	}//FOLD00

}

}
/*
public	class	App {

	public static void Main() {
		const	int	YUZHCABLE_BRANCHID	=	1011727;
		string		ConnectionString	=	"Server=harmattan;Database=Scrooge70test;Integrated Security=TRUE;";
		//string		ConnectionString	=	"Server=BOREAS;Database=Scrooge70;Integrated Security=TRUE;";
		CSc2Reports	Sc2Reports		= new	CSc2Reports();
		if	( Sc2Reports.Open( ConnectionString ) ) {
			// Sc2Reports.YuzhCable (  42582 , YUZHCABLE_BRANCHID , "uzkabel" );
			// Sc2Reports.CbCrdDoc(  42542 , YUZHCABLE_BRANCHID , "230616.CRD" );
			// Sc2Reports.TreeExtract(  42542 , 42542 , "JUR.1859" , "230616.TRB" );
			// Sc2Reports.Repozitory( "1" , "F:\\TRASH" , 42746 , 42746 , 42749 , false , true );
			// Sc2Reports.Repozitory( "2" , "dbo.RP_Repozitory_GetFile02" , "F:\\TRASH" , 42366 , 42366 , 42367 , true );
			// Sc2Reports.DocOfDay(42744,"110117.doc");
			// Sc2Reports.Balance("balance.txt",42714,42714,false);
			Sc2Reports.ConsolidatedBalance( "balance.txt" , 42774 , 42774 );
		}
		Sc2Reports.Close();
	}
}
*/