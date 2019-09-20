// Версия 1.003  от 16 декабря 2011 г.  - Классы для работы с двоичными файлами
using	DateTime	=	System.DateTime                 ;
using	Debug		=	System.Diagnostics.Debug	;
using	Debugger	=	System.Diagnostics.Debugger	;
using	Money		=	System.Decimal			;
using	MyTypes							;

namespace MyTypes
{
	public class CBinReader
	{
		System.IO.FileStream	HFile;

		int Current;

		public void Close()
		{
			if (HFile == null)
				return;
			try {
				HFile.Close();
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
			HFile = null;
		}

		public byte this[ int Index ] {
			get {
				if (Current != (-1)) {
					return ((byte)Current);
				} else {
					return 0;
				}
			}
		}

		public int ReadBlock(byte[] Buffer, int Count)
		{
			if (HFile == null)
				return 0;
			try {
				return HFile.Read(Buffer, 0, Count);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
			return 0;
		}

		public bool Read()
		{
			if (HFile == null)
				return false;
			try {
				Current = HFile.ReadByte();
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			if (Current == (-1)) {
				return false;
			}
			return true;
		}

		public bool Open(string FileName)
		{
			if (FileName == null)
				return false;
			if (FileName.Trim() == "")
				return false;
			if (!(HFile == null))
				Close();
			try {
				HFile = new System.IO.FileStream(FileName, System.IO.FileMode.Open, System.IO.FileAccess.Read);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

	}

	//--------------------------------------------------------
	public class CBinWriter
	{
		System.IO.FileStream	HFile;

		public void Close()
		{
			if (HFile == null)
				return;
			try {
				HFile.Close();
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
			HFile = null;
		}

		public bool WriteBlock(byte[] Buffer, int Count)
		{
			if (HFile == null)
				return false;
			try {
				HFile.Write(Buffer, 0, Count);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public bool Write(byte Value)
		{
			if (HFile == null)
				return false;
			try {
				HFile.WriteByte(Value);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public bool OpenForAppend(string FileName)
		{
			if (FileName == null)
				return false;
			if (FileName.Trim() == "")
				return false;
			if (!(HFile == null))
				Close();
			try {
				HFile = new System.IO.FileStream(FileName, System.IO.FileMode.Append, System.IO.FileAccess.Write);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public bool Create(string FileName)
		{
			if (FileName == null)
				return false;
			if (FileName.Trim() == "")
				return false;
			if (!(HFile == null))
				Close();
			try {
				HFile = new System.IO.FileStream(FileName, System.IO.FileMode.Append, System.IO.FileAccess.Write);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

	}
}