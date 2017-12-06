package corp.cencosud.commerce;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.sql.Connection;
//import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import com.ibm.as400.access.AS400;
import com.ibm.as400.access.AS400JDBCDriver;


public class InicioPrograma {

	private static BufferedWriter bw;
	private static String path;

	/**
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		Map <String, String> mapArguments = new HashMap<String, String>();
		String sKeyAux = null;

		for (int i = 0; i < args.length; i++) {

			if (i % 2 == 0) {

				sKeyAux = args[i];
			}
			else {

				mapArguments.put(sKeyAux, args[i]);
			}
		}

		try {

			File info              = null;
			File miDir             = new File(".");
			path                   =  miDir.getCanonicalPath();
			info                   = new File(path+"/info.txt");
			bw = new BufferedWriter(new FileWriter(info));
			info("El programa se esta ejecutando...");
			crearTxt(mapArguments);
			System.out.println("El programa finalizo.");
			info("El programa finalizo.");
			bw.close();
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
		}
	}

	private static void crearTxt(Map <String, String> mapArguments) {

		Connection dbconnection = crearConexion();
		File file1              = null;
		BufferedWriter bw       = null;
		PreparedStatement pstmt = null;
		StringBuffer sb         = null;
		int iFechaIni           = 0;

		try {

			try {

				iFechaIni = restarDia(mapArguments.get("-fi"));
				//iFechaIni = 201707203;
				//iFechaFin = restarDia(mapArguments.get("-ff"));

				//iFechaIni = restarDia("20160822");
				//iFechaIni = restarDia("201707203");
				
			}
			catch (Exception e) {
				e.printStackTrace();
			}
			info("[iFechaIni]"+iFechaIni);
			file1        = new File(path + "/Ecommerce-" + iFechaIni + ".txt");

			bw           = new BufferedWriter(new FileWriter(file1));
			
			ejecutarPaso1(dbconnection);
			ejecutarPaso2(iFechaIni, dbconnection);
			ejecutarPaso3(iFechaIni, dbconnection);
			ejecutarPaso4(iFechaIni, dbconnection);
			ejecutarPaso5(iFechaIni, dbconnection);
			
			bw.write("NUMORDEN;");
			bw.write("CODDESP;");
			bw.write("FECTRANTSL;");
			bw.write("NUMCTLTSL;");
			bw.write("NUMTERTSL;");
			bw.write("NUMTRANTSL;");
			bw.write("PxQ;");
			bw.write("SKU;");
			bw.write("CANVEND;");
			bw.write("ESTORDEN;");
			bw.write("SUBESTOC;");
			bw.write("TIPVTA;");
			bw.write("TIPOPAG; \n");
			//-- las ordenes que estan en ecommerce	
			sb = new StringBuffer();
			sb.append("SELECT ");
			sb.append("A.NUMORDEN, ");
			sb.append("A.CODDESP, ");
			sb.append("A.FECTRANTSL, ");
			sb.append("A.NUMCTLTSL, ");
			sb.append("A.NUMTERTSL, ");
			sb.append("A.NUMTRANTSL, ");
			sb.append("(B.CANVEND * B.PRECUNI) AS PxQ, ");
			sb.append("CONCAT(SUBSTR(B.CODSKU,1,6),SUBSTR(B.CODSKU,8,3)) as SKU, ");
			sb.append("B.CANVEND, ");
			sb.append("A.ESTORDEN, ");
			sb.append("A.SUBESTOC, ");
			sb.append("A.TIPVTA, ");
			sb.append("A.TIPOPAG ");
			sb.append("FROM CECEBUGD.SVVIF03 A INNER JOIN CECEBUGD.SVVIF04 B ON ");
			sb.append("(A.TIPVTA = B.TIPVTA) AND (A.NUMORDEN = B.NUMORDEN) ");
			sb.append("WHERE (((A.CODDESP) <>18) ");
			sb.append("AND ((A.FECTRANTSL) ");
			sb.append("Between ? AND ?) ");
			sb.append("AND ((A.ESTORDEN) = 99) ");
			sb.append("AND (A.SUBESTOC <> 99) ");
			sb.append("AND ((A.TIPVTA) = 1 ");
			sb.append("OR (A.TIPVTA) = 2 OR (A.TIPVTA) = 15)) ");
			//sb.append(" AND CONCAT(SUBSTR(B.CODSKU,1,6),SUBSTR(B.CODSKU,8,3))  NOT IN (943793999,834756999,967009999,696544999,704558001,875691999,785256999,995353015,995307045,995309026,995319012,995317013,995319010,995321018,995349011,995299020,995337017,995254030,995322002,995335036,995324028,995353022,995273015,995253030,995253002,995273044,995307002,995308002,995321023,995349014,995309056,995312043,995321013,995323016,995317023,995309002,995273033,995349024,995341018,995346002,995291026,995319014,995279030,995312039,995309027,995279013,995310021,995309029,995322010,995274002,995309025,995316019,995319021,995321002,995312040,995343002,995315032,995291019,523306016,995291002,995343014,995323029,995307014,995335017,995273014,995299027,995273053,995281030,995321012,995253031,995307022,995339036,995323026,995315015,995303033,995310002,995349022,995321022,995315027,995324027,995312030,995346014,995273002,995353012,995319002,995281018,995307049,995298032,995312036,995298016,995313002,995324011,995326002,995340029,995303053,995291024,995343017,995279002,995295016,995307052,995276002,995343021,995343015,995321016,995279014,995310043,995303018,995291021,995342012,995299002,995281028,995315002,995253033,995322022,995341010,995276011,995319019,995312025,995281032,995299037,995346021,995310017,995310049,995315022,995303038,995310011,995325014,995346019,995298026,995310030,995298033,995305032,995291018,995308025,995279016,995298037,995340011,995343016,995253011,995346003,995337026,995316022,995308013,995339037,995323014,995305036,995253024,995305010,995307047,995303016,995254013,995274014,995310018,995310047,995295011,995340019,995340027,995349010,995308083,995291025,995342002,995303013,995303039,995315013,995308042,995349002,995308033,995299021,995281013,995323017,995319015,995298017,995309028,995321021,995312064,995341020,995254016,995315016,995295002,995316014,995325002,995319011,995307017,995253014,995335010,995357024,995273024,995313010,995273032,995321019,995308082,995276014,995348010,995349018,523306011,995253021,995253025,995253040,995254018,995273026,995273034,995274013,995276028,995279012,995279028,995281017,995295019,995295031,995298002,995298040,995299025,995299028,995299048,995299051,995303014,995303034,995303036,995303055,995308018,995308061,995309013,995309017,995309019,995310015,995310039,995310044,995315021,995315024,995316010,995316025,995319028,995321025,995322015,995323028,995324026,995326003,995335002,995337010,995337014,995337015,995337022,995339002,995339019,995339035,995341019,995341022,995341024,995343019,995349019,995357013,995357018,995346016,665772999,995295020,419857999,567556999,850701999,104410002,644046007,175838999,995298011,812303999,158230999,995316023,995357019,999107999,909692999,588843999,206722999,995357045,995273011,995346017,995323027,768260999,770822999,995346018,930191999,995254017,995254021,995274011,995274016,995276010,995279011,995281016,995299013,995299034,995299049,995303011,995303012,995303045,995305002,995307030,995308023,995308024,995308075,995309012,995310027,995310032,995310040,995312017,995312062,995313012,995313017,995313031,995315012,995315025,995317019,995321015,995321050,995322013,995324025,995325010,995325022,995325025,995335022,995337013,995339023,995341026,995342021,995343011,995346010,995348002,995349023,995351016,995353011,995357002,336137999,523306014,947519999,995253034,995274010,995276013,995279015,995279031,995279037,995291012,995291022,995291023,995295003,995295022,995298014,995298029,995299018,995303037,995307032,995308019,995309033,995310036,995312027,995317010,995321020,995322016,995337002,995337012,995337019,995339015,995339020,995341012,995343022,995346011,995346013,995357017)  ");
			
			
			pstmt        = dbconnection.prepareStatement(sb.toString());
			pstmt.setInt(1, iFechaIni);
			pstmt.setInt(2, iFechaIni);
			ResultSet rs = pstmt.executeQuery();
			
			info("[Ini paso 6]");
			while (rs.next()) {
				int fechaS  = rs.getInt("FECTRANTSL");
				int loloca  = rs.getInt("NUMCTLTSL");
				int numTer  = rs.getInt("NUMTERTSL");
				int numTran = rs.getInt("NUMTRANTSL");
				
				if(!ejecutarPaso6(fechaS, loloca, numTer, numTran, dbconnection)){
					bw.write(rs.getString("NUMORDEN")+";");
					bw.write(rs.getString("CODDESP")+";");
					bw.write(rs.getString("FECTRANTSL")+";");
					bw.write(rs.getString("NUMCTLTSL")+";");
					bw.write(rs.getString("NUMTERTSL")+";");
					bw.write(rs.getString("NUMTRANTSL")+";");
					bw.write(rs.getString("PxQ")+";");
					bw.write(rs.getString("SKU")+";");
					bw.write(rs.getString("CANVEND")+";");
					bw.write(rs.getString("ESTORDEN")+";");
					bw.write(rs.getString("SUBESTOC")+";");
					bw.write(rs.getString("TIPVTA")+";");
					bw.write(rs.getString("TIPOPAG")+"\n");
				} /*else if (ejecutarPaso6(fechaS, loloca, numTer, numTran, dbconnection)){
					bw.write(rs.getString("NUMORDEN")+";");
					bw.write(rs.getString("CODDESP")+";");
					bw.write(rs.getString("FECTRANTSL")+";");
					bw.write(rs.getString("NUMCTLTSL")+";");
					bw.write(rs.getString("NUMTERTSL")+";");
					bw.write(rs.getString("NUMTRANTSL")+";");
					bw.write(rs.getString("PxQ")+";");
					bw.write(rs.getString("SKU")+";");
					bw.write(rs.getString("CANVEND")+";");
					bw.write(rs.getString("ESTORDEN")+";");
					bw.write(rs.getString("SUBESTOC")+";");
					bw.write(rs.getString("TIPVTA")+";");
					bw.write(rs.getString("TIPOPAG")+"\n");
					
					
					
				}*/
			}
			info("[Fin paso 6]");
			

			info("Archivos creados.");
		}
		catch (Exception e) {
			System.out.println(e.getMessage());
			info("[crearTxt1]Exception:"+e.getMessage());
		}
		finally {

			cerrarTodo(dbconnection,pstmt,bw);
		}
	}

	private static void ejecutarPaso1(Connection dbconnection){
		StringBuffer sb         = new StringBuffer();
		PreparedStatement pstmt = null;
		PreparedStatement pstmt2 = null;
		PreparedStatement pstmt3 = null;
		try {
			info("[Ini paso 1]");
			
			sb = new StringBuffer();
			sb.append("DELETE FROM BJPROYEC.JPDNOVRET");
			pstmt        = dbconnection.prepareStatement(sb.toString());
			info("registros eliminados de BJPROYEC.JPDNOVRET: "+pstmt.executeUpdate());	
			
			sb = new StringBuffer();
			sb.append("DELETE FROM BJPROYEC.JPDVTASDAD");
			pstmt2        = dbconnection.prepareStatement(sb.toString());
			info("registros eliminados de BJPROYEC.JPDVTASDAD: "+pstmt2.executeUpdate());
			
			sb = new StringBuffer();
			sb.append("DELETE FROM BJPROYEC.JPDTOTDAD");
			pstmt3        = dbconnection.prepareStatement(sb.toString());
			info("registros eliminados de BJPROYEC.JPDTOTDAD: "+pstmt3.executeUpdate());
			
			info("[Fin paso 1]");
			
		}
		catch (Exception e) {
			e.printStackTrace();
			info("[crearPaso1]Exception:"+e.getMessage());
		}
		finally {
			cerrarTodo(null,pstmt,null);
			cerrarTodo(null,pstmt2,null);
			cerrarTodo(null,pstmt3,null);
		}
	}
	
	private static void ejecutarPaso2(int fecha, Connection dbconnection){
		StringBuffer sb         = new StringBuffer();
		PreparedStatement pstmt = null;
		
		try {
			info("[Ini paso 2]");
			
			sb = new StringBuffer();
			sb.append("INSERT INTO BJPROYEC.JPDNOVRET ");
			sb.append("SELECT A.FECTRANTSL, A.NUMCTLTSL, A.NUMTERTSL, A.NUMTRANTSL, A.NUMCORRDUP, A.CTANOVTSL ");
			sb.append("FROM SVALBUGD.SVNOF81 a, SVALBUGD.SVNOF82 B ");
			sb.append("WHERE A.FECTRANTSl >= ? and A.FECTRANTSl <= ? ");
			sb.append("AND B.FECTRANTSl = A.FECTRANTSl ");
			sb.append("AND B.NUMCTLTSL  = A.NUMCTLTSL ");
			sb.append("AND B.NUMTERTSL  = A.NUMTERTSL ");
			sb.append("AND B.NUMTRANTSL = A.NUMTRANTSL ");
			sb.append("AND B.RETENIDO   = 'S' ");
			sb.append("GROUP BY A.FECTRANTSL,A.NUMCTLTSL, A.NUMTERTSL,A.NUMTRANTSL,A.NUMCORRDUP, A.CTANOVTSL");
			
			pstmt = dbconnection.prepareStatement(sb.toString());
			pstmt.setInt(1, fecha);
			pstmt.setInt(2, fecha);
			info("registros insertados en BJPROYEC.JPDNOVRET: "+pstmt.executeUpdate());

			info("[Fin paso 2]");
		}
		catch (Exception e) {
			e.printStackTrace();
			info("[crearPaso2]Exception:"+e.getMessage());
		}
		finally {
			cerrarTodo(null,pstmt,null);
		}
	}
	
	private static void ejecutarPaso3(int fecha, Connection dbconnection){
		StringBuffer sb         = new StringBuffer();
		PreparedStatement pstmt = null;
		
		try {
			info("[Ini paso 3]");
			
			sb = new StringBuffer();
			sb.append("INSERT INTO BJPROYEC.JPDVTASDAD ");
			sb.append("SELECT (A.FECTRANTSL * 1) AS FECHA ");
			sb.append(", (A.NUMCTLTSL * 1) AS TIENDA ");
			sb.append(", (A.NUMSOLGUI * 1) AS NUMERO_SD ");
			sb.append(", '                    ' AS OD_EOM ");
			sb.append(", A.FECTRANTSL ");
			sb.append(", A.NUMCTLTSL ");
			sb.append(", A.NUMTERTSL ");
			sb.append(", A.NUMTRANTSL ");
			sb.append(", A.NUMCORRDUP ");
			sb.append("FROM SVALBUGD.SVVSF00 A ");
			sb.append("LEFT JOIN BJPROYEC.JPDNOVRET B ");
			sb.append("ON A.FECTRANTSL = B.FECTRANTSL ");
			sb.append("AND A.NUMCTLTSL  = B.NUMCTLTSL ");
			sb.append("AND A.NUMTERTSL  = B.NUMTERTSL ");
			sb.append("AND A.NUMTRANTSL = B.NUMTRANTSL ");
			sb.append("WHERE A.FECTRANTSl >= ? AND A.FECTRANTSl <= ? ");
			sb.append("AND A.IDOCNULTSL = 0 ");
			sb.append("AND A.CCTRANTSL  < 10 ");
			sb.append("AND A.IBOLRETTSL < 6 ");
			sb.append("AND A.NUMSOLGUI > 0 ");
			
			pstmt = dbconnection.prepareStatement(sb.toString());
			pstmt.setInt(1, fecha);
			pstmt.setInt(2, fecha);
			info("registros insertados en BJPROYEC.JPDVTASDAD: "+pstmt.executeUpdate());

			info("[Fin paso 3]");
		}
		catch (Exception e) {
			e.printStackTrace();
			info("[crearPaso3]Exception:"+e.getMessage());
		}
		finally {
			cerrarTodo(null,pstmt,null);
		}
	}

	private static void ejecutarPaso4(int fecha, Connection dbconnection){
		StringBuffer sb         = new StringBuffer();
		PreparedStatement pstmt = null;
		
		try {
			info("[Ini paso 4]");
			
			sb = new StringBuffer();
			sb.append("INSERT INTO BJPROYEC.JPDVTASDAD ");
			sb.append("SELECT (A.FECTRANTSL * 1) AS FECHA, ");
			sb.append("(A.NUMCTLTSL * 1) AS TIENDA, ");
			sb.append("(0 * 1) AS NUMERO_SD, ");
			sb.append("A.NUMDOCEOM AS OD_EOM, ");
			sb.append("A.FECTRANTSL, A.NUMCTLTSL, ");
			sb.append("A.NUMTERTSL, A.NUMTRANTSL, ");
			sb.append("A.NUMCORRDUP ");
			sb.append("FROM SVALBUGD.SVLVF30 A ");
			sb.append("LEFT JOIN BJPROYEC.JPDNOVRET B ");
			sb.append("ON A.FECTRANTSL = B.FECTRANTSL ");
			sb.append("AND A.NUMCTLTSL  = B.NUMCTLTSL ");
			sb.append("AND A.NUMTERTSL  = B.NUMTERTSL ");
			sb.append("AND A.NUMTRANTSL = B.NUMTRANTSL ");
			sb.append("WHERE A.FECTRANTSl >= ? and A.FECTRANTSl <= ? ");
			sb.append("AND A.NUMDOCEOM > '00000000000000000000'");
			
			pstmt = dbconnection.prepareStatement(sb.toString());
			pstmt.setInt(1, fecha);
			pstmt.setInt(2, fecha);
			info("registros insertados en BJPROYEC.JPDVTASDAD: "+pstmt.executeUpdate());

			info("[Fin paso 4]");
		}
		catch (Exception e) {
			e.printStackTrace();
			info("[crearPaso4]Exception:"+e.getMessage());
		}
		finally {
			cerrarTodo(null,pstmt,null);
		}
	}
	
	private static void ejecutarPaso5(int fecha, Connection dbconnection){
		StringBuffer sb         = new StringBuffer();
		PreparedStatement pstmt = null;
		
		try {
			info("[Ini paso 5]");
			
			sb = new StringBuffer();
			sb.append("INSERT INTO BJPROYEC.JPDTOTDAD ");
			sb.append("SELECT A.FECHA, ");
			sb.append("B.LOLOCA, ");
			sb.append("B.lonivi, ");
			sb.append("A.numero_sd, ");
			sb.append("A.OD_EOM, ");
			sb.append("C.NUMCORRDUP, ");
			sb.append("(SUBSTR(DIGITS(C.CODARTTSL), 3, 9)) AS SKU, ");
			sb.append("C.TOTARTTSL, ");
			sb.append("C.DEPARTTSL, ");
			sb.append("C.CANARTTSL, ");
			sb.append("C.MARDESTSL, ");
			sb.append("C.NUMTERTSL, ");
			sb.append("C.NUMTRANTSL ");
			sb.append("from BJPROYEC.JPDVTASDAD A ");
			sb.append("inner Join MMSP4LIB.ITFLOCL2 B ");
			sb.append("ON  A.NUMCTLTSL = B.LOCONT ");
			sb.append("INNER jOIN SVALBUGD.SVVSF01 C ");
			sb.append("ON  A.FECTRANTSL = C.FECTRANTSL ");
			sb.append("AND A.NUMCTLTSL  = C.NUMCTLTSL ");
			sb.append("AND A.NUMTERTSL  = C.NUMTERTSL ");
			sb.append("AND A.NUMTRANTSL = C.NUMTRANTSL");
			
			pstmt = dbconnection.prepareStatement(sb.toString());
			info("registros insertados en BJPROYEC.JPDTOTDAD: "+pstmt.executeUpdate());

			info("[Fin paso 5]");
		}
		catch (Exception e) {
			e.printStackTrace();
			info("[crearPaso5]Exception:"+e.getMessage());
		}
		finally {
			cerrarTodo(null,pstmt,null);
		}
	}
	
	private static boolean ejecutarPaso6(int fechaS,int loloca, int numTer, int numTran, Connection dbconnection){
		StringBuffer sb         = null;
		PreparedStatement pstmt = null;
		boolean retorno         = true;
		
		try {
			
			sb = new StringBuffer();
			//Venta en real soporte venta
			sb.append("SELECT FECHA,LOLOCA,LONIVI,NUMERO_SD,OD_EOM,NUMTERTSL,NUMTRANTSL from BJPROYEC.JPDTOTDAD ");
			sb.append("WHERE FECHA = ? ");
			sb.append("and LOLOCA = ? ");
			sb.append("and NUMTERTSL = ? ");
			sb.append("and NUMTRANTSL = ? ");
			//sb.append(" AND sku NOT IN (523306011,696544999,704558001,785256999,834756999,875691999,967009999,995253021,995253024,995253025,995253030,995253040,995254018,995273015,995273026,995273034,995274013,995276028,995279002,995279012,995279028,995281017,995291002,995291019,995291026,995295002,995295019,995295031,995298002,995298040,995299002,995299025,995299027,995299028,995299048,995299051,995303014,995303016,995303018,995303034,995303036,995303053,995303055,995307049,995308013,995308018,995308061,995309002,995309013,995309017,995309019,995309026,995310015,995310017,995310021,995310039,995310044,995312039,995313010,995315013,995315021,995315024,995315032,995316010,995316014,995316019,995316025,995319002,995319010,995319012,995319021,995319028,995321002,995321021,995321023,995321025,995322002,995322015,995323028,995324026,995326003,995335002,995337010,995337014,995337015,995337022,995339002,995339019,995339035,995339036,995340011,995340029,995341010,995341019,995341022,995341024,995342012,995343014,995343015,995343019,995346014,995349014,995349019,995353015,995357013,995357018)  ");
			//sb.append(" AND SKU NOT IN (523306011,696544999,704558001,785256999,834756999,875691999,967009999,995253021,995253024,995253025,995253030,995253040,995254018,995273015,995273026,995273034,995274013,995276028,995279002,995279012,995279028,995281017,995291002,995291019,995291026,995295002,995295019,995295031,995298002,995298040,995299002,995299025,995299027,995299028,995299048,995299051,995303014,995303016,995303018,995303034,995303036,995303053,995303055,995307049,995308013,995308018,995308061,995309002,995309013,995309017,995309019,995309026,995310015,995310017,995310021,995310039,995310044,995312039,995313010,995315013,995315021,995315024,995315032,995316010,995316014,995316019,995316025,995319002,995319010,995319012,995319021,995319028,995321002,995321021,995321023,995321025,995322002,995322015,995323028,995324026,995326003,995335002,995337010,995337014,995337015,995337022,995339002,995339019,995339035,995339036,995340011,995340029,995341010,995341019,995341022,995341024,995342012,995343014,995343015,995343019,995346014,995349014,995349019,995353015,995357013,995357018,251637999,336137999,930191999,943793999,995253017,995253026,995253036,995253039,995254002,995254025,995273020,995274002,995276002,995276011,995279011,995279016,995281012,995281036,995291013,995298011,995298029,995299039,995303038,995307002,995307020,995307023,995307024,995307034,995307037,995307051,995308002,995308017,995308023,995310037,995310038,995312002,995312017,995312069,995315025,995315027,995317032,995321027,995321033,995321057,995322014,995322021,995322034,995324010,995324012,995324014,995325002,995325022,995325029,995340022,995340025,995341016,995342002,995343024,995343027,995346015,995346018,995349023,995349026,995351017,995353002,995353022,995357031,995357032,995357045) ");
			sb.append("group by FECHA,LOLOCA,LONIVI,NUMERO_SD,OD_EOM,NUMTERTSL,NUMTRANTSL ");
			
			pstmt = dbconnection.prepareStatement(sb.toString());
			pstmt.setInt(1, fechaS);
			pstmt.setInt(2, loloca);
			pstmt.setInt(3, numTer);
			pstmt.setInt(4, numTran);
			
			ResultSet rsSelect = pstmt.executeQuery();
			
			if (!rsSelect.next()) {
				retorno = false;
			}
		}
		catch (Exception e) {
			
			info("[crearPaso6]Exception:"+e.getMessage());
		}
		finally {
			cerrarTodo(null,pstmt,null);
		}
		
		return retorno;
	}
	
	private static Connection crearConexion() {

		System.out.println("Creando conexion a ROBLE.");
		AS400JDBCDriver d = new AS400JDBCDriver();
		String mySchema = "RDBPARIS2";
		Properties p = new Properties();
		AS400 o = new AS400("roble.cencosud.corp","usrcom", "usrcom");
		Connection dbconnection = null;

		try {

			dbconnection = d.connect (o, p, mySchema);
			System.out.println("[crearConexion()] Conexion a ROBLE CREADA.");
		}
		catch (Exception e) {

			System.out.println("[crearConexion()]"+e.getMessage());
		}
		return dbconnection;
	}
/*
	private static Connection crearConexionDB2() {
		
		System.out.println("Creando conexion a HUB.");
		Connection dbconnection = null;

		try {

			Class.forName("com.ibm.db2.jcc.DB2Driver");
			dbconnection = DriverManager.getConnection("jdbc:db2://spp36db04r:50051/PHUBP01","con_hubp","82ndy78hdjos");
			System.out.println("Conexion a HUB CREADA.");
		}
		catch (Exception e) {

			e.printStackTrace();
		}
		return dbconnection;
	}

	private static String limpiarCeros(String str) {

		int iCont = 0;

		while (str.charAt(iCont) == '0') {

			iCont++;
		}
		return str.substring(iCont, str.length());
	}
*/
	private static void cerrarTodo(Connection cnn, PreparedStatement pstmt, BufferedWriter bw){

		try {

			if (cnn != null) {
				cnn.close();
				cnn = null;
			}
		}
		catch (Exception e) {

			System.out.println(e.getMessage());
			info("[cerrarTodo]Exception:"+e.getMessage());
		}
		try {

			if (pstmt != null) {

				pstmt.close();
				pstmt = null;
			}
		}
		catch (Exception e) {

			System.out.println(e.getMessage());
			info("[cerrarTodo]Exception:"+e.getMessage());
		}
		try {

			if (bw != null) {

				bw.flush();
				bw.close();
				bw = null;
			}
		}
		catch (Exception e) {

			System.out.println(e.getMessage());
			info("[cerrarTodo]Exception:"+e.getMessage());
		}
	}

	private static void info(String texto){

		try {

			bw.write(texto+"\n");
			bw.flush();
		}
		catch (Exception e) {

			System.out.println("Exception:"+e.getMessage());
		}
	}

	private static int restarDia(String sDia) {

		int dia = 0;
		String sFormato = "yyyyMMdd";
		Calendar diaAux = null;
		String sDiaAux = null;
		SimpleDateFormat df = null;

		try {

			diaAux = Calendar.getInstance();
			df = new SimpleDateFormat(sFormato);
			diaAux.setTime(df.parse(sDia));
			diaAux.add(Calendar.DAY_OF_MONTH, -1);
			sDiaAux = df.format(diaAux.getTime());
			dia = Integer.parseInt(sDiaAux);
		}
		catch (Exception e) {

			info("[restarDia]Exception:"+e.getMessage());
		}
		return dia;
	}
}
