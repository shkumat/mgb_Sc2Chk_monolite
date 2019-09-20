// Версия 1.01  от 7 августа 2016 г. - Пинг адреса в сети
// https://msdn.microsoft.com/en-us/library/system.net.networkinformation.ping(v=vs.110).aspx
using	MyTypes;

namespace MyTypes {

	public	class	CPinger {
		System.Net.NetworkInformation.Ping	Pinger	= new	System.Net.NetworkInformation.Ping()	;
		System.Net.NetworkInformation.PingReply	Reply	;
		System.Net.NetworkInformation.PingOptions Options = new	System.Net.NetworkInformation.PingOptions();
		byte[]					Packet	=	System.Text.Encoding.ASCII.GetBytes("Test connection...");

		public	bool	Ping( string Address , int Timeout ) {
			if	( Address == null )
				return	false;
			Address		=	Address.Trim();
			if	( ( Address == "" ) || ( Timeout < 1 ) )
				return	false;
			Options.DontFragment	=	true;
			try {
				Reply		=	Pinger.Send( Address, Timeout, Packet, Options );
			}
			catch	( System.Exception Excpt ) {
				Err.Add(Excpt);
				return	false;
			}
			if	( Reply.Status != System.Net.NetworkInformation.IPStatus.Success )
				return	false;
			else
				return	true;
		}
	}
}