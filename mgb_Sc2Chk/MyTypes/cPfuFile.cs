// Версия 1.01  от 28 ноября 2016 г. Класс для выборки данных из файлов пенсионных фонндов
/*
		Структура информационной строки файла :
1	Номер счета			N19	1-19
2	Номер филиала			N5	20-24
3	Код вклада			N3	25-27
4	Сума (в коп.)			N19	28-46
5	ФИО				C100	47-146
6	Идентификационный номер		C10	147-156
7	День выплаты			C2(/C3)	157-158(/159)
8	CRLF				C2	159(/160)-160(/161)
*/
using	MyTypes;

namespace MyTypes {

	public	class	CPfuReader {

		const	int	MIN_DATALINE_LENGTH  =	156 ;	// минимально-допустимая длина строки данных
		string		Buffer_of_the_header =	CAbc.EMPTY;
		string		Buffer_of_the_record =	CAbc.EMPTY;
		CTextReader	TextReader	= new	CTextReader();

		bool	Is_DataLine_Valid () {
			if	( Buffer_of_the_record	== null )
				return	false ;
			if	( Buffer_of_the_record.Length < MIN_DATALINE_LENGTH )
				return	false ;
			return	true;
		}

		public	void	Close() {
			Buffer_of_the_header	=	CAbc.EMPTY;
			Buffer_of_the_record	=	CAbc.EMPTY;
			TextReader.Close();
		}

		public	bool	Open( string FileName ) {
			Close();
			if	( FileName == null )
				return	false;
			if	( ! TextReader.Open( FileName , CAbc.CHARSET_DOS ) )
				return	false;
			if	( ! TextReader.Read() )
				return	false;
			Buffer_of_the_header	=	TextReader.Value;
			return	true;
		}

		public	bool	Read() {
			Buffer_of_the_record	=	CAbc.EMPTY;
			do {
				if	( ! TextReader.Read() )
					return	false ;
				if	( TextReader.Value == null )
					return	false ;
			} while	( TextReader.Value.Length < MIN_DATALINE_LENGTH );
			Buffer_of_the_record	=	TextReader.Value ;
			return	true;
		}

		public	long	Cents() {
			if	( ! Is_DataLine_Valid() )
				return	0;
			return	CCommon.CLng( Buffer_of_the_record.Substring(27,19).Trim() );
		}

		public	string	AccountNum() {
			if	( ! Is_DataLine_Valid() )
				return	CAbc.EMPTY;
			return	Buffer_of_the_record.Substring(0,19).Trim().TrimStart('0');
		}

		public	string	ClientName() {
			if	( ! Is_DataLine_Valid() )
				return	CAbc.EMPTY;
			return	Buffer_of_the_record.Substring(46,100).Trim();
		}

		public	string	IdentCode() {
			if	( ! Is_DataLine_Valid() )
				return	CAbc.EMPTY;
			return	Buffer_of_the_record.Substring(146,10).Trim();
		}		

	}

}
/*
public	class	App {

	public	static void Main() {
		CPfuReader	PfuReader	= new	CPfuReader();
		if	( PfuReader.Open( "0000106.029" ) )
			while	( PfuReader.Read() )
				CCommon.Print( PfuReader.IdentCode() + "\t" + PfuReader.AccountNum() + "\t" + PfuReader.Cents().ToString() + "\t" + PfuReader.ClientName() );
		PfuReader.Close();
	}
}
*/