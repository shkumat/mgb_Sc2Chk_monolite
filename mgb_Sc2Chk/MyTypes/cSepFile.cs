// Версия 2.01 от 8 апреля 2019г. - Классы для работы с платежными файлами
using MyTypes;

namespace MyTypes {

	public sealed class CSepAFileInfo {
	
		public static int HEADER_SIZE = 298;
		public static int RECORD_SIZE = 594;
		public static readonly int[] Header_Field_Size = { 100,2,12,6,4,6,16,16,64,6,64,2};
		public static readonly int[] Record_Field_Size = { 9,14,9,14,1,16,2,10,3,6,6,38,38,160,20,20,20,3,2,14,14,9,16,64,34,34,16,2 };
		//------  Поля заголовка			
		public const int H_EMPTYSTR	= 0;	// char[100]; // Пеpвые 100 - пpобелы 			
		public const int H_CRLF1	= 1;	// char[  2]; // Символ концец строки                       			
		public const int H_FILENAME	= 2;	// char[ 12]; // Наименование  файла			
		public const int H_DATE 	= 3; 	// char[  6]; // Дата создания файла    			
		public const int H_TIME		= 4; 	// char[  4]; // Дата создания файла    			
		public const int H_STRCOUNT	= 5;	// char[  6]; // Количество ИС в файле  			
		public const int H_TOTALDEBET	= 6;	// char[ 16]; // Сумма дебета по файлу  			
		public const int H_TOTALCREDIT	= 7;	// char[ 16]; // Сумма кpедита по файлу 			
		public const int H_DES		= 8;	// char[ 64]; // ЕЦП                    			
		public const int H_DES_ID	= 9;	// char[  6]; // ID ключа ЕЦП           			
		public const int H_DES_OF_HEADER=10;	// char[ 64]; // ЕЦП заголовка          			
		public const int H_CRLF2	=11;	// char[  2]; // Символ `конец строки`
		//------  Поля информационной строки			
		public const int L_DEBITMFO	= 0;	// char[  9]; // Дебет-МФО			
		public const int L_DEBITACC	= 1;	// char[ 14]; // Дебет-счет			
		public const int L_CREDITMFO	= 2;	// char[  9]; // Кредит-МФО			
		public const int L_CREDITACC	= 3;	// char[ 14]; // Кредит счет			
		public const int L_FLAG		= 4;	// char[  1]; // Флаг `дебет/кредит`			
		public const int L_SUMA		= 5;	// char[ 16]; // Сумма в копейках			
		public const int L_DTYPE	= 6;	// char[  2]; // Вид документа			
		public const int L_NDOC		= 7;	// char[ 10]; // Номер документа			
		public const int L_CURRENCY	= 8;	// char[  3]; // Валюта			
		public const int L_DATE1	= 9;	// char[  6]; // Дата платежа			
		public const int L_DATE2	=10;	// char[  6]; // Дата пуступления документа			
		public const int L_DEBITNAME	=11;	// char[ 38]; // Наименование дебет-счета			
		public const int L_CREDITNAME	=12;	// char[ 38]; // Наименование кредит-счета			
		public const int L_PURPOSE	=13;	// char[160]; // Назначение платежа			
		public const int L_RESERVED1	=14;	// char[ 20]; // (резерв)  			
		public const int L_DEBITACC_EXT	=15;	// char[ 20]; // Расширенный лицевой счет клиента А 			
		public const int L_CREDITACC_EXT=16;	// char[ 20]; // Расширенный лицевой счет клиента Б 			
		public const int L_SYMBOL	=17;	// char[  3]; // Кассовый символ			
		public const int L_RESERVED2	=18;	// char[  2]; // (резерв)			
		public const int L_OKPO1	=19;	// char[ 14]; // Идент.код клиента А			
		public const int L_OKPO2	=20;	// char[ 14]; // Идент.код клиента Б			
		public const int L_ID		=21;	// char[  9]; // Идентификатор документа			
		public const int L_RESERVED3	=22; 	// char[ 16]; // (резерв)			
		public const int L_DES		=23; 	// char[ 64]; // ЕЦП			
		public const int L_DEBITIBAN	=24;	// char[ 34]; // Дебет-IBAN		
		public const int L_CREDITIBAN	=25;	// char[ 34]; // Кредит-IBAN		
		public const int L_RESERVED4	=26;	// char[ 16]; // (резерв)		
		public const int L_CRLF		=27; 	// char[  2]; // Символ `конец строки`
	}

	public class CSepAReader : CDatReader	, IFileOfColumnsReader {
		bool	IFileOfColumnsReader.Read() {
			return	base.Read();
                }

		string	IFileOfColumnsReader.this[ int Index ] {
	        get	{	
                        	return	base[ Index ];
                	}
                }

		int	 IFileOfColumnsReader.FieldCount {
        	get	{	
				return	CSepAFileInfo.Record_Field_Size.Length;
			}
	        }

		int	 IFileOfColumnsReader.Count {
        	get	{	
				return	CCommon.CInt32( Head( CSepAFileInfo.H_STRCOUNT ) );
			}
	        }

		bool IFileOfColumnsReader.Open( string FileName , int CharSet , params int[] MetaData ) {
			return	Open( FileName , CharSet );
		}

		public override bool Open(string FileName, int CharSet) {
			HeaderFieldSize = CSepAFileInfo.Header_Field_Size;
			RecordFieldSize = CSepAFileInfo.Record_Field_Size;
			if ((base.Open(FileName, CharSet) == true)) {
				if ((RecordSize == CSepAFileInfo.RECORD_SIZE) & (HeaderSize == CSepAFileInfo.HEADER_SIZE)) {
					return true;
				}
			}
			return false;
		}

	}

	public class CSepAWriter : CDatWriter {
		public override bool Create(string FileName, int CharSet) {
			HeaderFieldSize = CSepAFileInfo.Header_Field_Size;
			RecordFieldSize = CSepAFileInfo.Record_Field_Size;
			if ((base.Create(FileName, CharSet) == true)) {
				if ((RecordSize == CSepAFileInfo.RECORD_SIZE) & (HeaderSize == CSepAFileInfo.HEADER_SIZE)) {
					return true;
				}
			}
			return false;
		}

		public override bool OpenForAppend(string FileName, int CharSet) {
			HeaderFieldSize = CSepAFileInfo.Header_Field_Size;
			RecordFieldSize = CSepAFileInfo.Record_Field_Size;
			if ((base.OpenForAppend(FileName, CharSet) == true)) {
				if ((RecordSize == CSepAFileInfo.RECORD_SIZE) & (HeaderSize == CSepAFileInfo.HEADER_SIZE)) {
					return true;
				}
			}
			return false;
		}
	}

	// ----------------------------------------------------------------------
	public sealed class CSepVFileInfo {
		public static int HEADER_SIZE = 529;
		public static int RECORD_SIZE = 138;
		public static readonly int[] Header_Field_Size = { 100,2,12,6,4,6,16,16,365,2 }	;
		public static readonly int[] Record_Field_Size = { 9,14,9,14,1,16,2,10,3,6,9,12,6,12,6,1,4,1,1,2 };
		//------  Поля заголовка
		public const int H_EMPTYSTR	= 0;	// char[100]; // Пеpвые 100 - пpобелы
		public const int H_CRLF1	= 1;	// char[  2]; // Символы конца строки
		public const int H_FILENAME	= 2;	// char[ 12]; // Имя файла
		public const int H_DATE 	= 3; 	// char[  6]; // Дата создания файла
		public const int H_TIME		= 4; 	// char[  4]; // Время создания файла
		public const int H_STRCOUNT	= 5;	// char[  6]; // Количество ИС в файле
		public const int H_TOTALDEBET	= 6;	// char[ 16]; // Сумма дебета по файлу
		public const int H_TOTALCREDIT	= 7;	// char[ 16]; // Сумма кpедита по файлу
		public const int H_EMPTYSTR2	= 8;    // char[365]; // Неиспользуемое пространство
		public const int H_CRLF2	= 9;	// char[  2]; // Символы конец строки
		//------  Поля информационной строки
		public const int L_DEBITMFO	= 0;	// char[  9]; // Дебет-МФО
		public const int L_DEBITACC	= 1;	// char[ 14]; // Дебет-счет
		public const int L_CREDITMFO	= 2;	// char[  9]; // Кредит-МФО
		public const int L_CREDITACC	= 3;	// char[ 14]; // Кредит счет
		public const int L_FLAG		= 4;	// char[  1]; // Флаг `дебет/кредит`
		public const int L_SUMA		= 5;	// char[ 16]; // Сумма в копейках
		public const int L_DTYPE	= 6;	// char[  2]; // Вид документа
		public const int L_NDOC		= 7;	// char[ 10]; // Номер документа
		public const int L_CURRENCY	= 8;	// char[  3]; // Валюта
		public const int L_DATE1	= 9;	// char[  6]; // Дата платежа
		public const int L_ID		=10;	// char[  9]; // Идентификатор документа
		public const int L_FILENAME1	=11;	// char[ 12]; // Имя файла N 1
		public const int L_LINENUM1	=12;	// char[  6]; // Номер строки в файле N 1
		public const int L_FILENAME2	=13;	// char[ 12]; // Имя файла N 2
		public const int L_LINENUM2	=14;	// char[  6]; // Номер строки в файле N 2
		public const int L_STATUS	=15;	// char[  1]; // Флаг квитовки платежа
		public const int L_TIME		=16;	// char[  4]; // Время
		public const int L_NOL1		=17;	// char[  1]; //
		public const int L_NOL2		=18;	// char[  1]; //
		public const int L_CRLF		=19;	// char[  2]; // Символы конца строки
	}

	public class CSepVWriter : CDatWriter {

		public override bool Create(string FileName, int CharSet) {
			HeaderFieldSize = CSepVFileInfo.Header_Field_Size;
			RecordFieldSize = CSepVFileInfo.Record_Field_Size;
			if ( base.Create(FileName, CharSet) )
				if ( ( RecordSize == CSepVFileInfo.RECORD_SIZE ) && (HeaderSize == CSepVFileInfo.HEADER_SIZE) )
					return true;
			return false;
		}

		public override bool OpenForAppend(string FileName, int CharSet) {
			HeaderFieldSize = CSepVFileInfo.Header_Field_Size;
			RecordFieldSize = CSepVFileInfo.Record_Field_Size;
			if ( base.OpenForAppend(FileName, CharSet) == true)
				if ((RecordSize == CSepVFileInfo.RECORD_SIZE) & (HeaderSize == CSepVFileInfo.HEADER_SIZE))
					return true;
			return false;
		}
	}

/*
	public	class	MyDemo {

		public static void Main() {
			//Err.Debug();

			string	FILE_NAME	=	"D:\\!BCGC310.Z20";
			
			CSepAReader	A	= new	CSepAReader();
			if ( A.Open( FILE_NAME , 866 ) )
				while( A.Read() )
					CCommon.Print( A[ CSepAFileInfo.L_PURPOSE ] );
			A.Close();			
		}
	}
*/
}