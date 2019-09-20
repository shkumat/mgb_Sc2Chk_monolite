// Версия 1.013  от 5 октября 2015 г. - Классы для работы с текстовыми файлами
using	MyTypes;

namespace MyTypes
{
	public	interface IFileOfColumnsWriter
	{
		void	Close();
	        bool	Write( params string[] MetaData );
	        bool	WriteLine( params string[] MetaData );
	        bool	Create( string FileName , int CharSet , params int[] MetaData );
	}

	public	interface IFileOfColumnsReader
	{
	        bool	Read();
		void	Close();
		int	Count { get; }
		int	FieldCount { get; }
		string	this[ int Index ] { get; }
	        bool	Open( string FileName , int CharSet , params int[] MetaData );
	}

	//-----------------------------------------------------
	public class CTextFile : CArray
	{
		public	CTextFile(string FileName)
		{
			if (FileName == null)
				return;
			if (FileName.Trim() == "")
				return;
			LoadFromFile(FileName);
		}

		public	CTextFile(string FileName , int CharSet )
		{
			if (FileName == null)
				return;
			if (FileName.Trim() == "")
				return;
			LoadFromFile(FileName, CharSet );
		}
	}

	//----------------------------------------------------
	public class CTextReader
	{
		internal System.IO.StreamReader HFile;
		internal string Record;

		public	string	Value {
			get { 
				return Record; 
			}
		}

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

		public virtual bool Read()
		{
			if (HFile == null)
				return false;
			try {
				Record = HFile.ReadLine();
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			if (Record == null) {
				return false;
			} else {
				return true;
			}
		}

		public virtual bool Open(string FileName, int CharSet)
		{
			if ((FileName == null))
				return false;
			if ((FileName.Trim() == ""))
				return false;
			try {
				HFile = new System.IO.StreamReader(FileName, System.Text.Encoding.GetEncoding(CharSet));
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				HFile = null;
				return false;
			}
			return true;
		}

	}

	//----------------------------------------------------
	public class CTextWriter
	{
		internal System.IO.StreamWriter HFile;

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

		public virtual bool Add(string Value)
		{
			if (HFile == null)	return false;
			if (Value == null)	return false;
			try
			{
				HFile.Write(Value);
			}
			catch (System.Exception Excpt)
			{
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public bool Add(params object[] ObjectList)
		{
			if (ObjectList == null)
				return false;
			if (HFile == null)
				return false;			
			try {
				foreach ( object Obj in ObjectList) {
					if (!(Obj == null)) {
						if (!Add(Obj.ToString())) {
							return false;
						}
					}
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public bool Add(System.Collections.IEnumerable ObjectList)
		{
			if (ObjectList == null)
				return false;
			if (HFile == null)
				return false;			
			try {
				foreach ( object Obj in ObjectList) {
					if (!(Obj == null)) {
						if (!Add(Obj.ToString())) {
							return false;
						}
					}
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public virtual bool Create(string FileName, int CharSet)
		{
			if (FileName == null)
				return false;
			if (FileName.Trim().Length == 0)
				return false;
			if (HFile == null) {
			} else {
				Close();
			}
			try {
				HFile = new System.IO.StreamWriter(FileName, false, System.Text.Encoding.GetEncoding(CharSet));
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				HFile = null;
				return false;
			}
			return true;
		}

		public virtual bool OpenForAppend(string FileName, int CharSet)
		{
			if (FileName == null)
				return false;
			if (FileName.Trim().Length == 0)
				return false;
			if (HFile == null) {
			} else {
				Close();
			}
			try {
				HFile = new System.IO.StreamWriter(FileName, true, System.Text.Encoding.GetEncoding(CharSet));
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				HFile = null;
				return false;
			}
			return true;
		}

	}
/*
	public	class	MyDemo
	{
		public static void Main()
		{
			Err.Debug();

			string	FILE_NAME	=	"D:\\CTXTFILE.CS";
			
			CTextFile	TT	= new	CTextFile( FILE_NAME );
			foreach( string S in TT )
				CCommon.Print(S);

			CTextReader	T	= new CTextReader();
			if ( T.Open( FILE_NAME ,1251) )
				while(T.Read())
					CCommon.Print(T[0]);
			T.Close();
			
		}
	}
*/
}