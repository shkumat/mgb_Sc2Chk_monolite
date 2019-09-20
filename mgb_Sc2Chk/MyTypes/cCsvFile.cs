// Версия 1.14 от 31 октября 2016 г. - Классы для работы с файлами типа `строка с разделителями`
// Важно : столбцы нумеруются начиная с 1
using MyTypes;

namespace MyTypes
{
	public	class	CCsvWriter	:	CTextWriter ,	IFileOfColumnsWriter {
		int	FieldCounter	=	0;
		string	Delimiter	=	";";

		void	IFileOfColumnsWriter.Close() {
			base.Close();
		}

		bool	IFileOfColumnsWriter.Write( params string[] MetaData  ) {
			return	Add( MetaData );
		}

		bool	IFileOfColumnsWriter.WriteLine( params string[] MetaData ) {
			if	( Add( MetaData ) )
	                        return  Add( CAbc.CRLF );
			else
				return	false;
		}

		bool	IFileOfColumnsWriter.Create(string FileName, int CharSet,  params int[] MetaData) {
			if	( MetaData != null )
	        	        if	( MetaData.Length>0 )
		                	Delimiter	=  CCommon.Chr( MetaData[0] ).ToString();
			return base.Create(FileName, CharSet);
		}

		public	bool Add( params string[] MetaData ) {
			if	( HFile == null )
					return false;
			if	( MetaData == null )
					return	true;
                        for	( int CurrentField=0; CurrentField < MetaData.Length ; CurrentField++ ) {
				if	( MetaData[ CurrentField ] == CAbc.CRLF ) {
					FieldCounter=0;
					if	( ! base.Add( CAbc.CRLF ) ) 
						return	false;
					continue;
				}
				if	( 
						( MetaData[ CurrentField ].IndexOf( Delimiter ) >= 0  )
					||	( MetaData[ CurrentField ].IndexOf( CAbc.QUOTE ) >= 0  ) 
					||	( MetaData[ CurrentField ].IndexOf( "," ) >= 0  ) 
					||	( MetaData[ CurrentField ].IndexOf( "." ) >= 0  ) 
					) {
					if	( ! base.Add(
								( ( FieldCounter++ > 0 ) ? Delimiter : "" )
							,	CAbc.QUOTE
							,	MetaData[ CurrentField ].Replace( CAbc.QUOTE , CAbc.QUOTE+CAbc.QUOTE )
							,	CAbc.QUOTE
							)
						)
						return	false;
					continue;
				}
				if	( ! base.Add(
							( ( FieldCounter++ > 0 ) ? Delimiter : "" )
						,	MetaData[ CurrentField ] )
					)
					return	false;
			}
			return	true;
		}
	}

	public	class	CCsvReader	:	CTextReader ,	IFileOfColumnsReader
	{
		readonly string	DOUBLE_QUOTE	=	CAbc.QUOTE + CAbc.QUOTE ;
		readonly char	QuoteC		=	CAbc.QUOTE.ToCharArray()[0];
		int		TotalFields	=	0;
		int		TotalLines	=	0;
		int[]		Sizes			;
		int[]		Offsets			;
		string		Delimiter		;
		char		DelimiterC		;
		bool		UseThisDelimiter	;

		void	IFileOfColumnsReader.Close() {
			base.Close();
		}

		int	 IFileOfColumnsReader.Count {
        		get {
				return	TotalLines	;
			}
	        }

		int	 IFileOfColumnsReader.FieldCount {
        		get {
				return	TotalFields	;
			}
	        }

		//  Важно : столбцы нумеруются начиная с 1
		string	IFileOfColumnsReader.this[ int Index ] {
	        	get {
	        		string	Result		=	CAbc.EMPTY ;	
				if	( ( Index>0 ) && ( Index<=TotalFields ) )
					if	( ( Sizes[ Index-1 ] >0 )  && ( Offsets[ Index-1 ] >= 0 )  && ( ( Offsets[ Index-1 ] + Sizes[ Index-1 ] ) <= Record.Length ) )
						if	( ( Record[ Offsets[ Index-1 ] ] == QuoteC ) && ( Sizes[ Index-1 ]>2) )
							Result	=	Record.Substring( Offsets[ ( Index-1 ) ] + 1  , Sizes[ Index-1 ] - 2 ) ;
						else
							Result	=	Record.Substring( Offsets[ ( Index-1 ) ] , Sizes[ Index-1 ] ) ;
        			return	Result.Replace( DOUBLE_QUOTE , CAbc.QUOTE );
	                }
        	}

		bool	IFileOfColumnsReader.Open( string FileName , int CharSet , params int[] MetaData ) {
			TotalLines	=	0;
			TotalFields	=	0;
			if	( MetaData != null )
	        	        if( MetaData.Length>0 )
					Delimiter	=	CCommon.Chr( MetaData[0] ).ToString();
				else
					Delimiter	=	null;
			else
				Delimiter	=	null;
			if	( ! base.Open( FileName , CharSet ) )
				return	false;
			while	( base.Read() ) {
	                       	TotalLines++;
				AnalyzeIt();
	                }
                        base.Close();
			if	( ! base.Open( FileName , CharSet ) )
				return	false;
			if	( ( TotalFields == 0 ) || ( TotalLines == 0 ) )
				return	false;
			Sizes	= new	int[ TotalFields ];
			Offsets	= new	int[ TotalFields ];
			return	true ;
        	}

		void	AnalyzeIt() {
			if	( Record == null )
				return;
			if	( Record.Trim() == "" )
				return;
			if	( Delimiter == null  ) {
				Delimiter = ";" ;
				if	( Record.IndexOf( Delimiter ) < 1 )
					Delimiter	=	CAbc.TAB;
				else
					if	( Record.Length <= Record.IndexOf( Delimiter ) )
						Delimiter	=	CAbc.TAB;
					else
						if	( Record.IndexOf( Delimiter , Record.IndexOf( Delimiter ) + 1  ) < 1 )
							Delimiter	=	CAbc.TAB;
			}
			if	( Delimiter.Length == 0 )
				Delimiter = ";" ;
			DelimiterC	=	Delimiter.ToCharArray()[0];
			int	FieldCount	=	1	;
			UseThisDelimiter	=	true	;
			for	( int I=0 ; I<Record.Length ; I++ )
				if	( Record[ I ] ==  QuoteC )
					UseThisDelimiter	=	! UseThisDelimiter ;
				else
					if	( ( Record[ I ] ==  DelimiterC ) && ( UseThisDelimiter ) )
						FieldCount++;
			if	( FieldCount > TotalFields )
				TotalFields	=	FieldCount ;
		}

		bool	IFileOfColumnsReader.Read() {
	        	if	( ! base.Read() )
				return	false;
       	        	if	( Record==null )
				return	false;
			int	Index;
			for	( Index=0 ; Index<TotalFields ; Index++ ) {
				Offsets[ Index ] = -1 ; Sizes[ Index ] = -1 ;
			}
			Record	=	Record.Trim();
			if	( Record.Length == 0 )
				return	false;
			if	( Record.Length == 0 )
				return	false;
			UseThisDelimiter	=	true	;
			Offsets[0]		=	0 ;
			Sizes[0]		=	Record.Length ;
			int	FieldCount	=	0 ;
			for	( Index=0 ; Index<Record.Length ; Index++ ) {
				if	( Record[ Index ] ==  QuoteC )
					UseThisDelimiter	=	! UseThisDelimiter ;
				else
					if	( ( Record[ Index ] ==  DelimiterC ) && ( UseThisDelimiter ) && (FieldCount<TotalFields) ) {
						++FieldCount ;
						Offsets[ FieldCount ]	=	Index+1 ;
						Sizes[FieldCount ]	=	Record.Length - Index - 1;
						Sizes[ FieldCount - 1 ] =	Index-Offsets[ FieldCount - 1 ];
					}
			}
              		return	true;
        	}
	}
	/*		
	class	Application {
		public static void Main() {
			IFileOfColumnsWriter	Writer	= new	CCsvWriter();
			if	( Writer.Create("1.CSV" , CAbc.CHARSET_WINDOWS , 59) ) {
				Writer.Write("Мар"+CAbc.QUOTE+"яненко");
				Writer.Write("Iван");
				Writer.Write("Петрович");
				Writer.WriteLine("60,0000");
				Writer.Write("Column-1");
				Writer.Write("Column;2");
				Writer.Write("Column 3");
				Writer.Write("Column 4");
				Writer.WriteLine();
				Writer.WriteLine("Column	1", "Column;2", "Column 3", "Column 4");
			}
			Writer.Close();

			IFileOfColumnsReader	Reader = new	CCsvReader();
			Reader.Open("1.CSV", CAbc.CHARSET_WINDOWS );
			while	( Reader.Read() ) {
				for	( int Index=1 ; Index<=Reader.FieldCount ; Index++ )
					CCommon.Print( Reader[ Index ] );
				CCommon.Print("");
			}
			CCommon.Print("--------------");
			Reader.Close();
		}
	}
	*/	
}