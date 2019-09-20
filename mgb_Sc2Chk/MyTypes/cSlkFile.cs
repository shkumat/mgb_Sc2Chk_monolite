// Версия 1.03 от 7 сентября 2016 г. - Классы для работы с SLK-файлами
// Важно : столбцы нумеруются начиная с 1
using MyTypes;

namespace MyTypes
{
	public	class	CSlkWriter	:	CTextWriter ,	IFileOfColumnsWriter {
		const	string	HEADER	=	"ID;PCALCOOO32";
		int	LineCounter	=	1;
		int	FieldCounter	=	1;
		bool	HasHeaderWritten=	false;

		bool	IFileOfColumnsWriter.Write( params string[] MetaData  ) {
			return	Add( MetaData );
		}

		void	IFileOfColumnsWriter.Close() {
			if	( HasHeaderWritten )
				base.Add("E");
			base.Close();
		}

		bool	IFileOfColumnsWriter.WriteLine( params string[] MetaData ) {
			bool	Result	=	Add( MetaData ) ;
			LineCounter++;
			FieldCounter=1;
			return	Result;
		}

		bool	IFileOfColumnsWriter.Create(string FileName, int CharSet,  params int[] MetaData) {
			LineCounter	=	1;
			FieldCounter	=	1;
			return base.Create(FileName, CharSet);
		}

		public	bool Add( params string[] MetaData ) {
			if	( HFile == null )
					return false;
			if	( MetaData == null )
					return	true;
			if	( ! HasHeaderWritten ) {
				base.Add( HEADER );
				base.Add( CAbc.CRLF );
				HasHeaderWritten=true;
			}
                        for	( int CurrentField=0; CurrentField < MetaData.Length ; CurrentField++ )
				if	( MetaData[ CurrentField ] == CAbc.CRLF ) {
					FieldCounter=1;
					LineCounter++;
					return	base.Add( CAbc.CRLF );
				}
				else
					if	( CCommon.IsDigit( MetaData[ CurrentField ] ) ) {
						if	( ! base.Add(	"C;X"
								,	(FieldCounter++).ToString()
								,	";Y"
								,	LineCounter.ToString()
								,	";K"
								,	MetaData[ CurrentField ].Replace(",",".")
								,	CAbc.CRLF
								)
							)
							return	false;
					}
					else	{
						if	( ! base.Add(	"C;X"
								,	(FieldCounter++).ToString()
								,	";Y"
								,	LineCounter.ToString()
								,	";K"
								,	CAbc.QUOTE
								,	MetaData[ CurrentField ].Replace(";",";;")
								,	CAbc.QUOTE
								,	CAbc.CRLF
								)
							)
							return	false;
					}
			return	true;
		}
	}
/*
	// -------------------------------------------------------------------
	class	Application {
		public static void Main() {

			IFileOfColumnsWriter	Writer	= new	CSlkWriter();
			if	( Writer.Create("D:\\1.SLK" , CAbc.CHARSET_WINDOWS ) ) {
				Writer.Write("Column 1");
				Writer.Write("Column 2");
				Writer.Write("Column 3");
				Writer.WriteLine("4");
				Writer.Write("Column-1");
				Writer.Write("Column;2");
				Writer.Write("Column 3");
				Writer.Write("4");
				Writer.WriteLine();
				Writer.WriteLine("Column	1", "Column;2", "Column 3", "4");
			}
			Writer.Close();
		}
	}
*/
}