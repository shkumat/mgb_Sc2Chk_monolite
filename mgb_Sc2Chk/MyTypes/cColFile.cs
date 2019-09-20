// Версия 1.003  от 14 января 2015 г.  - Классы для работы с текстовыми файлами , содержащими столбцы фиксированной ширины
// Важно : столбцы нумеруются начиная с 1
using	MyTypes;

namespace MyTypes
{
	public	class	CColReader	: CTextReader , IFileOfColumnsReader
	{
		private	int	TotalLines	=	0;
        	private int[]	Sizes		=	{};

		bool	IFileOfColumnsReader.Read()
        	{
	        	return	 base.Read();
        	}

		void	IFileOfColumnsReader.Close()
        	{
	        	base.Close();
        	}

		int	IFileOfColumnsReader.Count
	        {
        		get	{	return	TotalLines	;	}
	        }

		int	IFileOfColumnsReader.FieldCount
	        {
        		get	{	return	Sizes.Length	;	}
	        }

		string	IFileOfColumnsReader.this[ int Index ]
	        {
	        get	{	
				int	I
				,	Offset	=	0;
				if( Record==null)
                                	return	CAbc.EMPTY ;
				if( Record.Length==0l)
        				return	CAbc.EMPTY ;
				if( Sizes==null)
        				return	CAbc.EMPTY ;
       				if( ( Sizes.Length>0 )  && ( Sizes.Length>=Index )  && ( Index>0 ) )
				{
					for(I=0;I<(Index-1);I++)
						Offset=Offset+Sizes[I];
					if( Record.Length <= Offset )
						return	CAbc.EMPTY ;
					if( Record.Length >= Offset+Sizes[Index-1] )
						return	Record.Substring(Offset,Sizes[Index-1]);
					else
						return	Record.Substring(Offset,Record.Length-Offset);
				}
        			return	CAbc.EMPTY ;
	                }
        	}

		bool IFileOfColumnsReader.Open( string FileName , int CharSet , params int[] MetaData )
	        {				
			TotalLines	=	0;
			Sizes		= new	int[1];
	                Sizes[0]	=	1;
			if( MetaData != null )
	        	        if( MetaData.Length>0 )
	                	{
		                	Sizes	= new	int[ MetaData.Length ];	
                                        CCommon.CopyArray(MetaData , Sizes );
                		}
			if( base.Open( FileName , CharSet ) )	
                	{
				while( base.Read() )
	                        	TotalLines++;
	                        base.Close();
        	                if( base.Open ( FileName , CharSet ) )
                			return	(TotalLines>0);
                        	else
                        		return	false;
	                }
			else
				return	false;
        	}
/*
		public static void Main()
		{		
        	        CCfgFile		CfgFile;
			IFileOfColumnsReader	FileReader	= new	CColReader();
			int[] 			MetaData	=	{ 16 , 24 , 18 , 1 , 17 };

        	        if( FileReader.Open( "C:\\TEMP\\Y7427_38.091" , 1251 , MetaData ) )
                		while( FileReader.Read() )
	                		CCommon.Print( FileReader[5] );
		}
*/
	}
}