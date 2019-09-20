// Версия 1.002  от 10 января 2013 г.  - Классы для работы с XML-файлами
using MyTypes;

namespace MyTypes
{
	public class CXmlReader
	{
		internal System.Xml.XmlTextReader HFile;

		internal System.Collections.ArrayList LayerNames;

		public bool HasText {
			get {
				if (HFile == null)
					return false;
				return HFile.HasValue;
			}
		}

		public bool HasAttributes {
			get {
				if (HFile == null)
					return false;
				if (HFile.AttributeCount == 0)
					return false;
				return true;
			}
		}

		public bool IsElementStart {
			get {
				if (HFile == null)
					return false;
				if ((HFile.NodeType == System.Xml.XmlNodeType.Element))
					return true;
				return false;
			}
		}

		public bool IsElementEnd {
			get {
				if (HFile == null)
					return false;
				if ((HFile.NodeType == System.Xml.XmlNodeType.EndElement))
					return true;
				return false;
			}
		}

		public string Text {
			get {
				if (HasText) {
					return HFile.Value;
				} else {
					return CAbc.EMPTY;
				}
			}
		}

		public string this[string AttributeName] {
			get {
				if (HFile == null)
					return CAbc.EMPTY;
				try {
					return HFile.GetAttribute(AttributeName.Trim());
				} catch (System.Exception Excpt) {
					Err.Add(Excpt);
				}
				return CAbc.EMPTY;
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
			LayerNames = null;
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
				LayerNames = new System.Collections.ArrayList();
				HFile = new System.Xml.XmlTextReader(FileName);
				HFile.WhitespaceHandling = System.Xml.WhitespaceHandling.None;
				LayerNames.Add(CAbc.EMPTY);
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public bool Read()
		{
			if (HFile == null)
				return false;
			try {
				if (HFile.Read()) {
					if (HFile.Depth > (LayerNames.Count - 1))
						LayerNames.Add(CAbc.EMPTY);
					if (HFile.NodeType == System.Xml.XmlNodeType.Element) {
						LayerNames[HFile.Depth] = HFile.Name.Trim().ToUpper();
					}
				} else {
					return false;
				}
			} catch (System.Exception Excpt) {
				Err.Add(Excpt);
				return false;
			}
			return true;
		}

		public string Path {
			get {
				if (HFile == null)
					return CAbc.EMPTY;
				if (LayerNames == null)
					return CAbc.EMPTY;
				System.Text.StringBuilder StrBuilder = new System.Text.StringBuilder();
				int I;
				int J = (HFile.Depth - 1);
				if ((HFile.NodeType == System.Xml.XmlNodeType.Element) | (HFile.NodeType == System.Xml.XmlNodeType.EndElement)) {
					J = J + 1;
				}
				for (I = 0; I <= J; I++) {
					StrBuilder.Append("/");
					StrBuilder.Append(LayerNames[I]);
				}
				return StrBuilder.ToString();
			}
		}
	/*
		public static void Main()
		{
			CXmlReader Reader = new CXmlReader();

			Reader.Open("D:\\A.XML");

			while (Reader.Read()) {
				if (Reader.IsElementStart) {
					System.Console.WriteLine("the start of " + Reader.Path);
					System.Console.WriteLine("attribute = " + Reader["SomeAttr"]);
				}

				if (Reader.HasText) {
					System.Console.WriteLine(Reader.Path + " = " + Reader.Text);
				}

			}
			Reader.Close();
		}
	*/
	}
}