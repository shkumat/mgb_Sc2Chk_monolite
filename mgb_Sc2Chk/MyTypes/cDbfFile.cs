// Версия 1.003  от 14 января 2015 г.  - Класс для работы с DBF-файлами
using MyTypes;

namespace MyTypes
{
	class CDbfReader : CDatReader, IFileOfColumnsReader
	{
		public	const	int	BUF32SIZE	=	32;
		private		int	TotalLines	=	0;
		private		int	TotalFields	=	0;
		private	string[]	RecordFieldName		;
		private	CBinReader BinReader 		= new	CBinReader();
		private	byte[]		HeaderBytes	= new byte[BUF32SIZE];

		bool IFileOfColumnsReader.Read()
		{
			return base.Read();
		}

		void IFileOfColumnsReader.Close()
		{
			base.Close();
		}

		int IFileOfColumnsReader.Count
		{
			get { return TotalLines; }
		}

		int IFileOfColumnsReader.FieldCount
		{
			get { return TotalFields; }
		}

		string IFileOfColumnsReader.this[int Index]
		{
			get { return base[Index]; }
		}

		public string this[string FieldName] {
			get {
				int Index;
				if (FieldName == null)
					return CAbc.EMPTY;
				FieldName = CCommon.Upper(CCommon.Trim(FieldName));
				if (FieldName.Length == 0)
					return CAbc.EMPTY;
				for (Index = 1; Index <= (HeaderFieldSize.Length - 1); Index++) {
					if ( FieldName == RecordFieldName[Index] ) {
						return this[Index];
					}
				}
				return CAbc.EMPTY;
			}
		}

		bool IFileOfColumnsReader.Open(string FileName, int CharSet, params int[] MetaData)
		{
			int I;
			TotalLines = 0;
			RecordSize = 0;
			HeaderSize = 0;
			if ((BinReader.Open(FileName))) {
				if ((BinReader.ReadBlock(HeaderBytes, BUF32SIZE) == BUF32SIZE)) {
				} else {
					BinReader.Close();
					return false;
				}
				TotalLines = (int)HeaderBytes[7];
				TotalLines = (TotalLines << 8) + (int)HeaderBytes[6];
				TotalLines = (TotalLines << 8) + (int)HeaderBytes[5];
				TotalLines = (TotalLines << 8) + (int)HeaderBytes[4];
				HeaderSize = (int)HeaderBytes[9];
				HeaderSize = (HeaderSize << 8) + (int)HeaderBytes[8];
				RecordSize = (int)HeaderBytes[11];
				RecordSize = (RecordSize << 8) + (int)HeaderBytes[10];
				TotalFields = ((HeaderSize - 1) >> 5) - 1;
				RecordFieldName = new string[TotalFields + 1] ;
				RecordFieldSize = new int[TotalFields + 1 ]	;
				HeaderFieldSize = new int[TotalFields + 2 ] ;
				RecordFieldSize[0] = 1;
				HeaderFieldSize[0] = BUF32SIZE;
				RecordFieldName[0] = "ALIVE";
				HeaderFieldSize[TotalFields + 1] = HeaderSize - ((TotalFields + 1) << 5);
				for (I = 1; I <= TotalFields; I++) {
					if ((BinReader.ReadBlock(HeaderBytes, BUF32SIZE) == BUF32SIZE)) {
					} else {
						BinReader.Close();
						return false;
					}
					HeaderFieldSize[I] = BUF32SIZE;
					RecordFieldSize[I] = (int)HeaderBytes[16];
					RecordSize = RecordSize - RecordFieldSize[I];
				}
				BinReader.Close();
			} else {
				BinReader.Close();
				return false;
			}
			RecordSize = RecordSize - RecordFieldSize[0];
			if (RecordSize == 0) {
				if (base.Open(FileName, CharSet)) {
					for (I = 1; I <= TotalFields; I++) {
						RecordFieldName[I] = CCommon.Upper(CCommon.Trim(Head(I).Substring(0, 10).Replace("\0", " ")));
					}
				} else {
					return false;
				}
			} else {
				return false;
			}
			return true;
		}

	}

}