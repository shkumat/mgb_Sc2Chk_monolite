// Версия 1.006  от 5 апреля 2013 г.  - Классы для работы с символьными файлами
using MyTypes;

namespace MyTypes
{
	//-------------------------------------------------------------
	public class CChrReader : CTextReader
	{

		char[] Buffer;
		internal string Header;
		internal int HeaderSize;
		internal int RecordSize;
		internal System.Text.StringBuilder StrBuilder	= new	System.Text.StringBuilder();

		public string Head()
		{
			return Header;
		}

		public override bool Open(string FileName, int CharSet )
		{
			if (HeaderSize < 1) {
				HeaderSize = 0;
			} 
			if (RecordSize < 1) {
				RecordSize = 1;
			} 
			if (base.Open(FileName, CharSet)) {
				try {
					if(HeaderSize>0)
					{
						Buffer = (char[]) System.Array.CreateInstance(typeof(System.Char), HeaderSize);
						System.Array.Clear(Buffer, 0, HeaderSize);
						if (HFile.ReadBlock(Buffer, 0, HeaderSize) != HeaderSize) 
						{
							return false;
						}
						StrBuilder.Length = 0;
						StrBuilder.Append(Buffer);
						Header = StrBuilder.ToString();
						if (Buffer.Length < RecordSize)
						{
							Buffer = (char[]) System.Array.CreateInstance(typeof(System.Char), RecordSize);
						}
					}
					else
					{
						Buffer = (char[]) System.Array.CreateInstance(typeof(System.Char), RecordSize);
					}
				} catch (System.Exception Excpt) {
					Err.Add(Excpt);
					return false;
				}
			} else {
				return false;
			}
			return true;
		}

		public override bool Read()
		{
			if (HFile == null)
				return false;
			try {
				if (Buffer == null) {
					Buffer = (char[]) System.Array.CreateInstance(typeof(System.Char), RecordSize);
				}
				if (Buffer.Length < RecordSize) {
					Buffer = (char[]) System.Array.CreateInstance(typeof(System.Char), RecordSize);
				}
				if (HFile.ReadBlock(Buffer, 0, RecordSize) != RecordSize) {
					return false;
				}
				StrBuilder.Length = 0;
				StrBuilder.Append(Buffer);
				Record = StrBuilder.ToString();
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

	}

	//-------------------------------------------------------------------
	public class CChrWriter : CTextWriter
	{
		internal int HeaderSize;
		internal int RecordSize;

		public bool WriteHeader(string Value)
		{
			if (HFile == null)	return false;
			if (Value == null)	return false;
			return	Add( Value.PadRight(HeaderSize).Substring(0,HeaderSize) );
		}

		public	bool WriteLine(string Value)
		{
			if (HFile == null)	return false;
			if (Value == null)	return false;
			return	Add( Value.PadRight(RecordSize).Substring(0,RecordSize) );			
		}

		public bool Create(string FileName, int CharSet, int Header_Size, int Record_Size)
		{
			if (Header_Size < 1) {
				HeaderSize = 0;
			} else {
				HeaderSize = Header_Size;
			}
			if (Record_Size < 1) {
				RecordSize = 1;
			} else {
				RecordSize = Record_Size;
			}
			return base.Create(FileName, CharSet);
		}

		public bool OpenForAppend(string FileName, int CharSet, int Header_Size, int Record_Size)
		{
			if (Header_Size < 1) {
				HeaderSize = 0;
			} else {
				HeaderSize = Header_Size;
			}
			if (Record_Size < 1) {
				RecordSize = 1;
			} else {
				RecordSize = Record_Size;
			}
			return base.OpenForAppend(FileName, CharSet);
		}
	}
}