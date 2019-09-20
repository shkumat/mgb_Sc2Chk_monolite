// Версия 1.13  от 23 сентября 2016г.  - Классы для работы с массивами в памяти
using	DateTime	=	System.DateTime                 ;
using	Debug		=	System.Diagnostics.Debug	;
using	Debugger	=	System.Diagnostics.Debugger	;
using	Money		=	System.Decimal			;
using	MyTypes							;
using	Node		=	System.Xml.XmlNode		;
using	NodeAtr		=	System.Xml.XmlAttribute		;

namespace MyTypes
{

	public class CArray : System.Collections.ArrayList
	{
		private object HFile;

		public override string ToString()
		{
			System.Text.StringBuilder StrBuilder = new System.Text.StringBuilder();
			int I;
			for (I = 0; I <= (Count - 1); I++) {
				StrBuilder.Append(this[I].ToString());
				StrBuilder.Append(System.Environment.NewLine);
			}

			return StrBuilder.ToString();
		}

		public void Add(string Value)
		{	
			base.Add(Value);
		}

		public void Add(System.Collections.IEnumerable ObjectList)
		{
			if (ObjectList == null)
				return;
			try {
				foreach ( object Obj in ObjectList ) {
					if (!(Obj == null)) {
						try {
							base.Add(Obj.ToString());
						} catch (System.Exception Excpt) {
							Err.Add(Excpt);
						}
					}
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
			return;
		}

		public void Add(params object[] ObjectList)
		{
			if (ObjectList == null)
				return;
			if (ObjectList.Length == 0)
				return;
			try {
				foreach ( object Obj in ObjectList ) {
					if (!(Obj == null)) {
						base.Add(Obj);
					}
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
		}

		public	bool	AppendFromFile( string FileName, int CharSet )
		{
			if (FileName == null)
				return false;
			if (FileName.Length == 0)
				return false;
			try {
				HFile = new System.IO.StreamReader(FileName, System.Text.Encoding.GetEncoding(CharSet));
				string AnyLine;
				do {
					AnyLine = ( (System.IO.StreamReader) HFile).ReadLine();
					if (!(AnyLine == null))
						Add(AnyLine);
				} while (!(AnyLine == null));
				( (System.IO.StreamReader) HFile).Close();
				HFile = null;
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public bool LoadFromFile(string FileName, int CharSet) 
		{
			Clear();
			return	AppendFromFile( FileName, CharSet );
		}

		public bool LoadFromFile(string FileName)
		{
			return LoadFromFile(FileName, CAbc.CHARSET_WINDOWS);
		}

		public bool AppendFromFile(string FileName)
		{
			return AppendFromFile(FileName, CAbc.CHARSET_WINDOWS);
		}

		public bool SaveToFile(string FileName, int CharSet)
		{
			if (FileName == null)
				return false;
			if (FileName.Length == 0)
				return false;
			try {
				HFile = new System.IO.StreamWriter(FileName, false, System.Text.Encoding.GetEncoding(CharSet));
				((System.IO.StreamWriter)HFile).Write(ToString());
				((System.IO.StreamWriter)HFile).Close();
				HFile = null;
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public bool SaveToFile(string FileName)
		{
			return SaveToFile(FileName, CAbc.CHARSET_WINDOWS);
		}

		public bool AppendToFile(string FileName, int CharSet)
		{
			if (FileName == null)
				return false;
			if (FileName.Length == 0)
				return false;
			try {
				HFile = new System.IO.StreamWriter(FileName, true, System.Text.Encoding.GetEncoding(CharSet));
				((System.IO.StreamWriter)HFile).Write(ToString());
				((System.IO.StreamWriter)HFile).Close();
				HFile = null;
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public bool AppendToFile(string FileName)
		{
			return AppendToFile(FileName, CAbc.CHARSET_WINDOWS);
		}

	}

	//-----------------------------------------------------------------
	public class CDictionary : System.Collections.DictionaryBase
	{

		private object HFile;

		public int Length()
		{
			return Dictionary.Keys.Count;
		}

		public bool Contains(string Key)
		{
			return Dictionary.Contains(Key.Trim().ToUpper());
		}

		public System.Collections.ICollection Keys()
		{
			return Dictionary.Keys;
		}

		public System.Collections.ICollection Values()
		{
			return Dictionary.Values;
		}

		public object this[string Key] {
			get { return Dictionary[Key.Trim().ToUpper()]; }
		}

		public bool Remove(string Key)
		{
			try {
				Dictionary.Remove(Key.Trim().ToUpper());
			} catch (System.Exception Excpt) {
				return false;
			}
			return true;
		}

		public override string ToString()
		{
			System.Text.StringBuilder StrBuilder = new System.Text.StringBuilder();
			foreach ( string AnyKey in Keys() ) {
				StrBuilder.Append(AnyKey + " = " + this[AnyKey]);
				StrBuilder.Append(System.Environment.NewLine);
			}
			return StrBuilder.ToString();
		}

		public bool Add(string Key, string Value)
		{
			if (Key == null)
				return false;
			if (Key.Trim().Length == 0)
				return false;
			if (Value == null) 
				Value = "";
			/*
			else
				if (Value.IndexOf(";") > 0)
					Value = Value.Substring(0, Value.IndexOf(";") ).Trim();
			*/
			try {
				Dictionary.Add(Key.Trim().ToUpper(), Value.Trim());
			} catch (System.Exception Excpt) {
				return false;
			}
			return true;
		}

		public bool Add(string Value)
		{
			if (Value == null)
				return false;
			try {
				Value = Value.Trim().Replace("=", " ").Replace(CAbc.TAB, " ");
				if (Value.IndexOf(" ") > 0) {
					Add(Value.Substring(0, Value.IndexOf(" ")).Trim(), Value.Substring(Value.IndexOf(" ")).Trim());
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
                                return false;
			}
                        return  true;
		}

		public bool AppendFromFile(string FileName, int CharSet)
		{
			if (FileName == null)
				return false;
			if (FileName.Length == 0)
				return false;
			try {
				HFile = new System.IO.StreamReader(FileName, System.Text.Encoding.GetEncoding(CharSet));
				string AnyLine;
				do {
					AnyLine = ((System.IO.StreamReader)HFile).ReadLine();
					if (AnyLine != null) {
						if (AnyLine.Trim().Length > 2) {
							if (AnyLine.Trim().Substring(0, 1) != ";") {
								Add(AnyLine);
							}
						}
					}
				} while (!(AnyLine == null));
				((System.IO.StreamReader)HFile).Close();
				HFile = null;
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public bool AppendFromFile(string FileName)
		{
			return AppendFromFile(FileName, CAbc.CHARSET_WINDOWS);
		}

		public bool LoadFromFile(string FileName, int CharSet) 
		{
			Clear();
			return	AppendFromFile( FileName, CharSet );
		}

		public bool LoadFromFile(string FileName)
		{
			return LoadFromFile(FileName, CAbc.CHARSET_WINDOWS);
		}

		public bool SaveToFile(string FileName, int CharSet)
		{
			if (FileName == null)
				return false;
			if (FileName.Length == 0)
				return false;
			try {
				HFile = new System.IO.StreamWriter(FileName, false, System.Text.Encoding.GetEncoding(CharSet));
				((System.IO.StreamWriter)HFile).Write(ToString());
				((System.IO.StreamWriter)HFile).Close();
				HFile = null;
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public bool SaveToFile(string FileName)
		{
			return SaveToFile(FileName, CAbc.CHARSET_WINDOWS);
		}

		public bool AppendToFile(string FileName, int CharSet)
		{
			if (FileName == null)
				return false;
			if (FileName.Length == 0)
				return false;
			try {
				HFile = new System.IO.StreamWriter(FileName, true, System.Text.Encoding.GetEncoding(CharSet));
				((System.IO.StreamWriter)HFile).Write(ToString());
				((System.IO.StreamWriter)HFile).Close();
				HFile = null;
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public bool AppendToFile(string FileName)
		{
			return AppendToFile(FileName, CAbc.CHARSET_WINDOWS);
		}

	}

	//-----------------------------------------------------------------
	public class CQueue : System.Collections.Queue
	{

		private object HFile;

		public override string ToString()
		{
			System.Text.StringBuilder StrBuilder = new System.Text.StringBuilder();
			foreach ( object Obj in ToArray() )
                                if (!(Obj == null))
                        	{
				StrBuilder.Append(Obj.ToString());
				StrBuilder.Append(System.Environment.NewLine);
				}
			return StrBuilder.ToString();
		}

		public void Add(string Value)
		{	
			base.Enqueue(Value);
		}

		public void Add(System.Collections.IEnumerable ObjectList)
		{
			if (ObjectList == null)
				return;
			try {
				foreach ( object Obj in ObjectList ) {
					if (!(Obj == null)) {
						base.Enqueue(Obj.ToString());
					}
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
		}

		public void Add(params object[] ObjectList)
		{
			if (ObjectList == null)
				return;
			if (ObjectList.Length == 0)
				return;
			try {
				foreach ( object Obj in ObjectList ) {
					if (!(Obj == null)) {
						base.Enqueue(Obj);
					}
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
		}

		public bool LoadFromFile(string FileName, int CharSet)
		{
			if (FileName == null)
				return false;
			if (FileName.Length == 0)
				return false;
			try {
				Clear();
				HFile = new System.IO.StreamReader(FileName, System.Text.Encoding.GetEncoding(CharSet));
				string AnyLine;
				do {
					AnyLine = ((System.IO.StreamReader)HFile).ReadLine();
					if (!(AnyLine == null))
						Add(AnyLine);
				} while (!(AnyLine == null));
				((System.IO.StreamReader)HFile).Close();
				HFile = null;
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public bool LoadFromFile(string FileName)
		{
			return LoadFromFile(FileName, CAbc.CHARSET_WINDOWS);
		}

		public bool SaveToFile(string FileName, int CharSet)
		{
			if (FileName == null)
				return false;
			if (FileName.Length == 0)
				return false;
			try {
				HFile = new System.IO.StreamWriter(FileName, false, System.Text.Encoding.GetEncoding(CharSet));
				((System.IO.StreamWriter)HFile).Write(ToString());
				((System.IO.StreamWriter)HFile).Close();
				HFile = null;
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public bool SaveToFile(string FileName)
		{
			return SaveToFile(FileName, CAbc.CHARSET_WINDOWS);
		}

		public bool AppendToFile(string FileName, int CharSet)
		{
			if (FileName == null)
				return false;
			if (FileName.Length == 0)
				return false;
			try {
				HFile = new System.IO.StreamWriter(FileName, true, System.Text.Encoding.GetEncoding(CharSet));
				((System.IO.StreamWriter)HFile).Write(ToString());
				((System.IO.StreamWriter)HFile).Close();
				HFile = null;
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public bool AppendToFile(string FileName)
		{
			return AppendToFile(FileName, CAbc.CHARSET_WINDOWS);
		}

	}

	//-----------------------------------------------------------------
	public class CStack : System.Collections.Stack
	{

		public void Add(string Value)
		{	
			base.Push(Value);
		}

		public void Push(System.Collections.IEnumerable ObjectList)
		{
			if (ObjectList == null)
				return;
			try {
				foreach ( object Obj in ObjectList ) {
					if (!(Obj == null)) {
						base.Push(Obj);
					}
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
		}

		public void Push(params object[] ObjectList)
		{
			if (ObjectList == null)
				return;
			if (ObjectList.Length == 0)
				return;
			try {
				foreach ( object Obj in ObjectList ) {
					if (!(Obj == null)) {
						base.Push(Obj);
					}
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
		}

	}

	//-----------------------------------------------------------------
	public class CXml : System.Xml.XmlDocument
	{

		private System.Xml.XmlTextWriter Writer;

		private System.Xml.XmlTextReader Reader;

		CXml(string Value)
		{
			if (Value == null)
				return;
			if (Value.Trim().Length == 0)
				return;
			try {
				base.LoadXml(Value);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
		}

		public override string ToString()
		{
			return base.InnerXml;
		}

		public System.Xml.XmlNode CreateNode(string NodeName,string NamespaceName)
		{
			try {
				return base.CreateNode("element", NodeName, NamespaceName);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return null;
			}
		}

		public bool SaveToFile(string FileName, int CharSet)
		{
			try {
				Writer = new System.Xml.XmlTextWriter(FileName, System.Text.Encoding.GetEncoding(CharSet));
				Writer.Formatting = System.Xml.Formatting.Indented;
				base.Save(Writer);
				Writer.Close();
				Writer = null;
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public bool SaveToFile(string FileName)
		{
			return SaveToFile(FileName, CAbc.CHARSET_WINDOWS);
		}

		public bool LoadFromFile(string FileName)
		{
			try {
				Reader = new System.Xml.XmlTextReader(FileName);
				Reader.MoveToContent();
				base.Load(Reader);
				Reader.Close();
				Reader = null;
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

	}

	//-----------------------------------------------------------------
	public struct Text : System.Collections.IList
	{

		private object HFile;
		private System.Collections.ArrayList Buffer;
		private System.Text.StringBuilder StrBuilder;

		private void Validate()
		{
			if (Buffer == null) {
				Buffer = new System.Collections.ArrayList();
				StrBuilder = new System.Text.StringBuilder();
			}
		}

		void System.Collections.IList.RemoveAt(int Index)
		{
			try {
				Buffer.RemoveAt(Index);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
		}

		bool System.Collections.IList.IsFixedSize {
			get { return false; }
		}

		bool System.Collections.IList.IsReadOnly {
			get { return false; }
		}

		int System.Collections.ICollection.Count {
			get { return Buffer.Count; }
		}

		bool System.Collections.ICollection.IsSynchronized {
			get { return false; }
		}

		object System.Collections.ICollection.SyncRoot {
			get { return this; }
		}

		void System.Collections.IList.Remove(object Value)
		{
			if (Value == null)
				return;
			try {
				Buffer.Remove(Value);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
		}

		bool System.Collections.IList.Contains(object Value)
		{
			Validate();
			if (Value == null)
				return false;
			try {
				return Buffer.Contains(Value);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
			return false;
		}

		System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
		{
			Validate();
			return Buffer.GetEnumerator();
		}

		void System.Collections.IList.Clear()
		{
			try {
				Buffer.Clear();
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
		}

		void System.Collections.ICollection.CopyTo(System.Array AnyArray, int Index)
		{
			Validate();
			if (AnyArray == null)
				return;
			try {
				Buffer.CopyTo(AnyArray, Index);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
		}

		public override string ToString()
		{
			Validate();
			StrBuilder.Length = 0;
			int I;
			for (I = 0; I <= ( Buffer.Count - 1); I++) {
				StrBuilder.Append(Buffer[I].ToString());
				StrBuilder.Append(System.Environment.NewLine);
			}
			return StrBuilder.ToString();
		}

		void System.Collections.IList.Insert(int Index, object Value)
		{
			Validate();
			if (Index < 0)
				return;
			if (Value == null)
				return;
			try {
				Buffer.Insert(Index, Value.ToString());
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
		}

		int System.Collections.IList.IndexOf(object Value)
		{
			Validate();
			if (Value == null)
				return (-1);
			try {
				return Buffer.IndexOf(Value.ToString());
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
			return (-1);
		}

		int System.Collections.IList.Add(object Value)
		{
			Validate();
			if (Value == null) {
				return (-1);
			}
			try {
				return Buffer.Add(Value.ToString());
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
			return (-1);
		}

		public int Add(System.Collections.IEnumerable ObjectList)
		{
			Validate();
			int Result=0;
			if (ObjectList == null)
				return (-1);
			try {
				foreach ( object Obj in ObjectList ) {
					if (!(Obj == null)) {
						Result = Buffer.Add(Obj.ToString());
					}
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return (-1);
			}
			return Result;
		}

		public int Add(params object[] ObjectList)
		{
			int Result=0;
			Validate();
			if (ObjectList == null)
				return (-1);
			if (ObjectList.Length == 0)
				return (-1);
			try {
				foreach ( object Obj in ObjectList ) {
					if (!(Obj == null)) {
						Result = Buffer.Add(Obj.ToString());
					}
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return (-1);
			}
			return Result;
		}

		object System.Collections.IList.this[int Index] {
			get {
				Validate();
				if ((Index > Buffer.Count ) || (Index < 0)) {
					return null;
				}
				try {
					return Buffer[Index];
				} catch (System.Exception Excpt) {
					Err.Add(Excpt);
				}
				return null;
			}
			set {
				Validate();
				if (value == null)
					return;
				try {
					Buffer[Index] = value.ToString();
				} catch (System.Exception Excpt) {
					Err.Add(Excpt);
				}
			}
		}

		public bool LoadFromFile(string FileName, int CharSet)
		{
			Validate();
			if (FileName == null)
				return false;
			if (FileName.Length == 0)
				return false;
			try {
				Buffer.Clear();
				HFile = new System.IO.StreamReader(FileName, System.Text.Encoding.GetEncoding(CharSet));
				string AnyLine;
				do {
					AnyLine = ((System.IO.StreamReader)HFile).ReadLine();
					if (!(AnyLine == null))
						Add(AnyLine);
				} while (!(AnyLine == null));
				((System.IO.StreamReader)HFile).Close();
				HFile = null;
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public bool LoadFromFile(string FileName)
		{
			return LoadFromFile(FileName, CAbc.CHARSET_WINDOWS);
		}

		public bool SaveToFile(string FileName, int CharSet)
		{
			Validate();
			if (FileName == null)
				return false;
			if (FileName.Length == 0)
				return false;
			try {
				HFile = new System.IO.StreamWriter(FileName, false, System.Text.Encoding.GetEncoding(CharSet));
				((System.IO.StreamWriter)HFile).Write(ToString());
				((System.IO.StreamWriter)HFile).Close();
				HFile = null;
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public bool SaveToFile(string FileName)
		{
			return SaveToFile(FileName, CAbc.CHARSET_WINDOWS);
		}

		public bool AppendToFile(string FileName, int CharSet)
		{
			Validate();
			if (FileName == null)
				return false;
			if (FileName.Length == 0)
				return false;
			try {
				HFile = new System.IO.StreamWriter(FileName, true, System.Text.Encoding.GetEncoding(CharSet));
				((System.IO.StreamWriter)HFile).Write(ToString());
				((System.IO.StreamWriter)HFile).Close();
                                HFile=null;
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public bool AppendToFile(string FileName)
		{
			return AppendToFile(FileName, CAbc.CHARSET_WINDOWS);
		}

	}

	//-----------------------------------------------------------------
	public struct VarBinary : System.Collections.IList
	{

		private object HFile;

		private System.IO.MemoryStream Buffer;

		private void Validate()
		{
			if (Buffer == null) {
				Buffer = new System.IO.MemoryStream();
			}
		}

		void System.Collections.IList.RemoveAt(int Index)
		{
			Delete(Index);
		}

		public int Length()
		{
			Validate();
			return (int)Buffer.Length;
		}

		public string ToString(int CharSet)
		{
			Validate();
			try {
				return System.Text.Encoding.GetEncoding(CharSet).GetString(Buffer.ToArray());
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return null;
			}
		}

		public override string ToString()
		{
			return ToString(CAbc.CHARSET_WINDOWS);
		}

		bool System.Collections.IList.IsFixedSize {
			get { return false; }
		}

		bool System.Collections.IList.IsReadOnly {
			get { return false; }
		}

		bool System.Collections.ICollection.IsSynchronized {
			get { return false; }
		}

		object System.Collections.ICollection.SyncRoot {
			get { return this; }
		}

		int System.Collections.ICollection.Count {
			get { return Length(); }
		}

		public bool Delete(int Position)
		{
			try {
                        	Validate();
				Buffer.SetLength( ( Length() )- 1);
                        } catch (System.Exception Excpt) {
                                Err.Add(Excpt);
                                return  false;
			}
                        return  true;
		}

		void System.Collections.IList.Clear()
		{
			Validate();
			try {
				Buffer.SetLength(0);
			} catch (System.Exception Excpt) {
			}
		}

		int System.Collections.IList.IndexOf(object Value)
		{
			Validate();
			try {
				return ToString().IndexOf(Value.ToString());
			} catch (System.Exception Excpt) {
				return (-1);
			}
			return (-1);
		}

		public int LastIndexOf(object Value)
		{
			Validate();
			try {
				return ToString().LastIndexOf(Value.ToString());
			} catch (System.Exception Excpt) {
				return (-1);
			}
			return (-1);
		}

		void System.Collections.IList.Remove(object Value)
		{
                        if (Value==null)
                                return;
                        while ( ToString().IndexOf(Value.ToString()) >= 0 ) {
				Delete( ToString().IndexOf(Value.ToString()) );
			}
		}

		bool System.Collections.IList.Contains(object Value)
		{
			Validate();
                        if      (Value==null)
                                return  false;
			if ( ToString().IndexOf(Value.ToString()) >= 0) {
				return true;
			} else {
				return false;
			}
		}

		System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
		{
			Validate();
			EnumeratorByte Result = new EnumeratorByte();
			try {
				Result.Bytes = Buffer.ToArray();
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
			return Result;
		}

                int AddByte(byte Value)
                {
			Validate();
			try {
				if (Value == null) {
					return (-1);
				} else {
					Buffer.WriteByte((byte)Value);
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
			return ( (Length()) - 1);
                }

		int System.Collections.IList.Add(object Value)
		{
                        return  AddByte((byte) Value);
		}

		void System.Collections.IList.Insert(int Index, object Value)
		{
			Validate();
			long SavedPos;
       			try {
       				SavedPos = Buffer.Position;
       				Buffer.Position = (long)Index;
       				Buffer.WriteByte((byte)Value);
       				Buffer.Position = SavedPos;
       			}
			catch (System.Exception Excpt) {
                                Err.Add( Excpt ) ;
			}
		}

		object System.Collections.IList.this[int Index] {
			get {
				Validate();
				if (Index > Length())
					return null;
				long SavedPos;
				byte Result;
				try {
					SavedPos = Buffer.Position;
					Buffer.Position = (long) Index;
					Result = (byte)Buffer.ReadByte();
					Buffer.Position = SavedPos;
				} catch (System.Exception Excpt) {
					Err.Add(Excpt);
					return null;
				}
				return Result;
			}
			set {
				Validate();
				long SavedPos;
				try {
					SavedPos = Buffer.Position;
					Buffer.Position = (long)Index;
					Buffer.WriteByte((byte)value);
					Buffer.Position = SavedPos;
				}
                                catch (System.Exception Excpt) {
				}
			}
		}

		void System.Collections.ICollection.CopyTo(System.Array AnyArray, int Index)
		{
			Validate();
			long J = (long)Index , I = 0 , SavedPos ;
			try {
				SavedPos = Buffer.Position;
				for (I = 0; I <= Buffer.Length ; I++) {
	       				Buffer.Position = J;
					AnyArray.SetValue( (byte)Buffer.ReadByte(), (int)J );
					J = J + 1;
				}
       				Buffer.Position = SavedPos;
			} catch (System.Exception Excpt) {
                                Err.Add(Excpt) ;
			}
		}

		public bool LoadFromFile(string FileName)
		{
			Validate();
			if (FileName == null)
				return false;
			if (FileName.Length == 0)
				return false;
			int NextByte,Result;
			try {
                                Buffer.SetLength(0);
				HFile = new System.IO.FileStream(FileName, System.IO.FileMode.Open, System.IO.FileAccess.Read);
				NextByte = ((System.IO.FileStream)HFile).ReadByte();
				while (NextByte != (-1)) {
					Result=AddByte((byte)NextByte);
					NextByte = ((System.IO.FileStream)HFile).ReadByte();
				}
				((System.IO.FileStream)HFile).Close();
				HFile = null;
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public bool SaveToFile(string FileName)
		{
			Validate();
			if (FileName == null)
				return false;
			if (FileName.Length == 0)
				return false;
			try {
				HFile = new System.IO.FileStream(FileName, System.IO.FileMode.Create, System.IO.FileAccess.Write);
				((System.IO.FileStream)HFile).Write(Buffer.ToArray(), 0, Length() );
				((System.IO.FileStream)HFile).Close();
				HFile = null;
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public bool AppendToFile(string FileName)
		{
			Validate();
			if (FileName == null)
				return false;
			if (FileName.Length == 0)
				return false;
			try {
				HFile = new System.IO.FileStream(FileName, System.IO.FileMode.Append, System.IO.FileAccess.Write);
				((System.IO.FileStream)HFile).Write(Buffer.ToArray(), 0, Length() );
				((System.IO.FileStream)HFile).Close();
				HFile = null;
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

	}

	//-----------------------------------------------------------------
	public struct VarChar : System.Collections.IList
	{

		private object HFile;

		private System.Text.StringBuilder Buffer;

		private void Validate()
		{
			if (Buffer == null) {
				Buffer = new System.Text.StringBuilder();
			}
		}

		void System.Collections.IList.RemoveAt(int Index)
		{
			Delete(Index);
		}

		public int Length()
		{
			Validate();
			return Buffer.Length;
		}

		public override string ToString()
		{
			Validate();
			return Buffer.ToString();
		}

		bool System.Collections.IList.IsFixedSize {
			get { return false; }
		}

		bool System.Collections.IList.IsReadOnly {
			get { return false; }
		}

		bool System.Collections.ICollection.IsSynchronized {
			get { return false; }
		}

		object System.Collections.ICollection.SyncRoot {
			get { return this; }
		}

		int System.Collections.ICollection.Count {
			get { return Length(); }
		}

		void System.Collections.IList.Remove(object Value)
		{       if      (Value==null)
                                return;
                	int     Position;
			try {
                        	Position=ToString().IndexOf(System.Convert.ToChar(Value));
				while (Position >= 0) {
					Delete(Position);
                	                Position=ToString().IndexOf(System.Convert.ToChar(Value));
				}
			} catch (System.Exception Excpt) {
                                Err.Add(Excpt);
			}
		}

		void System.Collections.ICollection.CopyTo(System.Array AnyArray, int Index)
		{
			Validate();
			int J = Index;
			int I = 0;
			try {
				for (I = 0; I <= Length() ; I++) {
					AnyArray.SetValue( Buffer[I] , J);
					J = J + 1;
				}
			} catch (System.Exception Excpt) {
                                Err.Add(Excpt);
			}
		}

		System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
		{
			Validate();
			EnumeratorChar Result = new EnumeratorChar();
			if ( ( Length() ) > 1) {
				try {
					Result.Chars = Buffer;
				} catch (System.Exception Excpt) {
					Err.Add(Excpt);
				}
			}
			return Result;
		}

		int System.Collections.IList.Add(object Value)
		{
			Validate();
			try {
				if (Value == null) {
					return (-1);
				} else {
					Buffer.Append(Value.ToString());
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return 0;
			}
			return ( (Length() ) - 1);
		}


		public int Add(System.Collections.IEnumerable ObjectList)
		{
			Validate();
			if (ObjectList == null) {
				return (-1);
			}
			try {
				foreach ( object Obj in ObjectList ) {
					if (!(Obj == null)) {
						Buffer.Append(Obj.ToString());
					}
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return (-1);
			}
			return ( (Length()) - 1 );
		}

		public int Add(params object[] ObjectList)
		{
			Validate();
			if (ObjectList == null) {
				return (-1);
			}
			try {
				foreach ( object Obj in ObjectList ) {
					if (!(Obj == null)) {
						Buffer.Append(Obj.ToString());
					}
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return (-1);
			}
			return ( (Length()) - 1 );
		}

		void System.Collections.IList.Clear()
		{
			Validate();
			try {
				Buffer.Length = 0;
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
		}

		int System.Collections.IList.IndexOf(object Value)
		{
			Validate();
			if (Value == null)
				return (-1);
			try {
				return ToString().IndexOf(System.Convert.ToChar(Value));
			} catch (System.Exception Excpt) {
                                Err.Add(Excpt);
				return (-1);
			}
			return (-1);
		}

		public int LastIndexOf(string ObjForFind)
		{
			Validate();
			if (ObjForFind == null)
				return (-1);
			try {
				return Buffer.ToString().LastIndexOf(ObjForFind);
			} catch (System.Exception Excpt) {
				return (-1);
			}
			return (-1);
		}

		bool System.Collections.IList.Contains(object Value)
		{
			Validate();
			try {
				if ( ToString().IndexOf(System.Convert.ToChar(Value)) > 0 )
                                	return	true;
                                else	return	false;
			} catch (System.Exception Excpt) {
                                Err.Add(Excpt);
				return false;
			}
                        return  false;
		}

		void System.Collections.IList.Insert(int Index, object Value)
		{
			Validate();
			try {
				Buffer.Insert(Index, Value);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
			}
		}

		public bool Replace(string OldObj, string NewObj)
		{
			Validate();
			try {
				Buffer.Replace(OldObj, NewObj);
			} catch (System.Exception Excpt) {
				return false;
			}
			return true;
		}

		public bool Delete(int Position, int Length)
		{
			Validate();
			try {
				Buffer.Remove(Position, Length);
			} catch (System.Exception Excpt) {
				return false;
			}
			return true;
		}

		public bool Delete(int Position)
		{
			return	Delete(Position, 1);
		}

		object System.Collections.IList.this[ int Index ] {
			get {
				Validate();
				if (Index < Length()) {
					try {
						return Buffer[Index];
					} catch (System.Exception Excpt) {
						Err.Add(Excpt);
						return null;
					}
				} else {
					return null;
				}
			}
			set {
				Validate();
				if (Index < Length()) {
					try {
						Buffer[Index] = System.Convert.ToChar(value);
					} catch (System.Exception Excpt) {
						Err.Add(Excpt);
					}
				}
			}
		}

		public bool LoadFromString(object Value)
		{
			Buffer = null;
			if (Value == null)
				return false;
			try {
				Buffer = new System.Text.StringBuilder(Value.ToString());
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				Validate();
				return false;
			}
			Validate();
			return true;
		}

		public bool LoadFromFile(string FileName, int CharSet)
		{
			Validate();
			if (FileName == null)
				return false;
			if (FileName.Length == 0)
				return false;
			bool Result;
			try {
				Buffer.Length=0;
				HFile = new System.IO.StreamReader(FileName);
				Result = (Add(((System.IO.StreamReader)HFile).ReadToEnd()) > 0);
				((System.IO.StreamReader)HFile).Close();
				HFile = null;
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return Result;
		}

		public bool LoadFromFile(string FileName)
		{
			return LoadFromFile(FileName, CAbc.CHARSET_WINDOWS);
		}

		public bool SaveToFile(string FileName, int CharSet)
		{
			Validate();
			if (FileName == null)
				return false;
			if (FileName.Length == 0)
				return false;
			try {
				HFile = new System.IO.StreamWriter(FileName, false, System.Text.Encoding.GetEncoding(CharSet));
				((System.IO.StreamWriter)HFile).Write(Buffer.ToString());
				((System.IO.StreamWriter)HFile).Close();
				HFile = null;
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public bool SaveToFile(string FileName)
		{
			return SaveToFile(FileName, CAbc.CHARSET_WINDOWS);
		}

		public bool AppendToFile(string FileName, int CharSet)
		{
			Validate();
			if (FileName == null)
				return false;
			if (FileName.Length == 0)
				return false;
			try {
				HFile = new System.IO.StreamWriter(FileName, false, System.Text.Encoding.GetEncoding(CharSet));
				((System.IO.StreamWriter)HFile).Write(Buffer.ToString());
				((System.IO.StreamWriter)HFile).Close();
				HFile = null;
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public bool AppendToFile(string FileName)
		{
			return AppendToFile(FileName, CAbc.CHARSET_WINDOWS);
		}

	}

	//-----------------------------------------------------------------	
	//	CParam	-  Словарь с параметрами командной строки
	//	Переметры должны передваваться в формате PowerShell :
	//	prog.exe   -param1  Value1   -param2  Value2   -param2  Value3  ...	
	public	class	CParam : CDictionary {

		string	ParamName	=	CAbc.EMPTY;

		public	string this[ string Key ] {
			get {
				if	( Key == null )
					return	CAbc.EMPTY;
				if	( Dictionary[ Key.Trim().ToUpper() ] == null )
					return	CAbc.EMPTY;
				return	Dictionary[ Key.Trim().ToUpper() ].ToString();
			}
		}

		public	CParam () {
			if	( CCommon.ParamCount() > 2 )
				for	( int Ptr = 1 ; Ptr < CCommon.ParamCount() ; Ptr++ )
					if	( ParamName != CAbc.EMPTY ) {
				 		base.Add( ParamName , CAbc.ParamStr[ Ptr ] );
				 		ParamName	=	"";
			 		} 
			 		else	{
				 		ParamName	=	CAbc.ParamStr[ Ptr ].Trim();
				 		if	( ParamName.Length > 1 )
				 			if	( ParamName.Substring(0,1)=="-" )
				 				ParamName	=	ParamName.Substring(1);
			 				else
			 					ParamName	=	CAbc.EMPTY;
				 		else
				 			ParamName	=	CAbc.EMPTY; 
				 	}
		}	
	}
	
}