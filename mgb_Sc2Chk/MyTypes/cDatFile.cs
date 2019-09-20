// Версия 1.007  от 5 апреля 2013 г.  - Классы для работы со структурными файлами
using MyTypes;

namespace MyTypes
{

	public class CDatReader : CChrReader
	{
		public int[] HeaderFieldSize;
		public int[] RecordFieldSize;

		public string this[int FieldNo]
		{
			get
			{	
				return	Line( FieldNo );
			}
		}

		public	string Line( int FieldNo )
		{
			if (RecordFieldSize == null)
				return CAbc.EMPTY;
			if ( (FieldNo < 0) || ( FieldNo > ( RecordFieldSize.Length - 1 ) ) )
				return CAbc.EMPTY;
			int I;
			int J = 0;
			for (I = 0; I <= (FieldNo - 1); I++)
			{
				J = J + RecordFieldSize[I];
			}
			return	Record.Substring(J, RecordFieldSize[FieldNo]);
		}

		public string Head(int FieldNo)
		{
			if (HeaderFieldSize == null)
				return CAbc.EMPTY;
			if ( (FieldNo < 0) || ( FieldNo > ( HeaderFieldSize.Length - 1 ) ) )
				return CAbc.EMPTY;
			int I;
			int J = 0;
			for (I = 0; I <= (FieldNo - 1); I++) 
			{
				J = J + HeaderFieldSize[I];
			}
			return	Header.Substring(J, HeaderFieldSize[FieldNo]);
		}
		
		public override bool Open(string FileName, int CharSet)
		{
			int Header_Size = 0;
			int Record_Size = 0;
			if (HeaderFieldSize == null) {
				Header_Size = 0;
			} else {
				foreach ( int AnySize in HeaderFieldSize) {
					Header_Size += AnySize;
				}
			}
			if (RecordFieldSize == null) {
				Record_Size = 1;
			} else {
				foreach ( int AnySize in RecordFieldSize) {
					Record_Size += AnySize;
				}
			}
			HeaderSize	=	Header_Size ;
			RecordSize	=	Record_Size ;
			return base.Open(FileName, CharSet);
		}
	}

	//-------------------------------------------------------------
	public class CDatWriter : CChrWriter
	{
		public	string[]		Head			;
		public	string[]		Line			;
		internal int[]			HeaderFieldSize		;
		internal int[]			RecordFieldSize		;
		System.Text.StringBuilder	StrBuilder		;

		public	CDatWriter() : base()
		{
			StrBuilder = new System.Text.StringBuilder();
		}

		public bool WriteHeader()
		{
			if (Head == null)	return false;
			if (HeaderSize == 0)	return false;
			return WriteHeader(GetHeader());
		}

		public bool WriteLine()
		{
			if (Line == null)	return false;
			if (RecordSize == 0)	return false;
			return WriteLine(GetFullLine());
		}

		public string GetHeader()
		{
			StrBuilder.Length = 0;
			int Field;
			for (Field = 0; Field <= (HeaderFieldSize.Length - 1); Field++)
			{
				if (Head[Field] == null)	Head[Field] = " ";
				if (CCommon.IsNumeric(Head[Field]))
				{
					StrBuilder.Append(Head[Field].PadLeft(HeaderFieldSize[Field]).Substring(0,HeaderFieldSize[Field]));
				}
				else
				{
					StrBuilder.Append(Head[Field].PadRight(HeaderFieldSize[Field]).Substring(0,HeaderFieldSize[Field]));
				}
			}
			return StrBuilder.ToString();
		}

		public string GetFullLine()
		{
			StrBuilder.Length = 0;
			int Field;
			for (Field = 0; Field <= (RecordFieldSize.Length - 1); Field++)
			{
				if (Line[Field] == null)	Line[Field] = " ";
				if (CCommon.IsNumeric(Line[Field]))
				{
					StrBuilder.Append(Line[Field].PadLeft(RecordFieldSize[Field]).Substring(0,RecordFieldSize[Field]));
				}
				else
				{
					StrBuilder.Append(Line[Field].PadRight(RecordFieldSize[Field]).Substring(0,RecordFieldSize[Field]));
				}
			}
			return StrBuilder.ToString();
		}

		public virtual bool Create(string FileName, int CharSet)
		{
			int Header_Size = 0;
			int Record_Size = 0;
			if (HeaderFieldSize == null) {
				Header_Size = 0;
			} else {
				foreach ( int AnySize in HeaderFieldSize) {
					Header_Size += AnySize;
				}
				if (HeaderFieldSize.Length > 0) {
					Head = ( string[] ) System.Array.CreateInstance(typeof(System.String), HeaderFieldSize.Length);
				}
			}
			if (RecordFieldSize == null) {
				Record_Size = 1;
			} else {
				foreach ( int AnySize in RecordFieldSize) {
					Record_Size += AnySize;
				}
				if (RecordFieldSize.Length > 0) {
					Line = ( string[] ) System.Array.CreateInstance(typeof(System.String), RecordFieldSize.Length);
				}
			}
			return base.Create(FileName, CharSet, Header_Size, Record_Size);
		}

		public virtual bool OpenForAppend(string FileName, int CharSet)
		{
			int Header_Size = 0;
			int Record_Size = 0;
			if (HeaderFieldSize == null) {
				Header_Size = 0;
			} else {
				foreach ( int AnySize in HeaderFieldSize) {
					Header_Size += AnySize;
				}
				if (HeaderFieldSize.Length > 0) {
					Head = ( string[] ) System.Array.CreateInstance(typeof(System.String), HeaderFieldSize.Length);
				}
			}
			if (RecordFieldSize == null) {
				Record_Size = 1;
			} else {
				foreach ( int AnySize in RecordFieldSize) {
					Record_Size += AnySize;
				}
				if (RecordFieldSize.Length > 0) {
					Line = ( string[] ) System.Array.CreateInstance(typeof(System.String), RecordFieldSize.Length);
				}
			}
			return base.OpenForAppend(FileName, CharSet, Header_Size, Record_Size);
		}

	}
}