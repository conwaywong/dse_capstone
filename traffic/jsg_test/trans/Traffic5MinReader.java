import org.apache.log4j.Logger;
import org.apache.log4j.Level;
import org.jetel.component.AbstractGenericTransform;
import org.jetel.data.*;
import org.jetel.metadata.*;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.*;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.data.sequence.*;

import java.io.*;
import java.util.*;
import java.text.*;
import java.util.zip.GZIPInputStream;

/**
 * This is an example custom reader. It shows how you can
 *  create records using a data source.
 */
public class Traffic5MinReader extends AbstractGenericTransform {

	@Override
	public void execute() throws ComponentNotReadyException {
		DateFormat format = new SimpleDateFormat("MM/dd/yyyy HH:mm:ss", Locale.ENGLISH);
		DataRecord rstation = outRecords[0],
				   rlane = outRecords[1],
				   robso = outRecords[2];
		Logger outlog = getLogger();
		String fUrl = getProperties().getStringProperty("InputFile"),
			   input, rtype;
		Sequence oidseq = getGraph().getSequence(getProperties().getStringProperty("SeqName"));
		File folder = getFile(fUrl);

		//int rcnt = 0;
		int fcnt, sfcnt, lfcnt, ofcnt, lcnt;
		long oid, sid = -1;

		DataField field;

		outlog.log(Level.INFO, "Record0 NumFields: " + rstation.getNumFields());
		outlog.log(Level.INFO, "Record1 NumFields: " + rlane.getNumFields());
		outlog.log(Level.INFO, "Record2 NumFields: " + robso.getNumFields());
		outlog.log(Level.INFO, "InputFolder = " + folder.getName());
		outlog.log(Level.INFO, "SeqName = " + oidseq.getName());
		
		for(final File file : folder.listFiles())
		{
			if(file.isDirectory() || !file.toString().endsWith(".gz"))
				continue;
			
			outlog.log(Level.INFO, "Inputfile = " + file.getName());

			//try (BufferedReader br = new BufferedReader(new FileReader(file)))
			try(BufferedReader br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file)))))
			{
				while((input = br.readLine()) != null)// && rcnt < 3)
				{
					//rcnt++;
					
					fcnt = sfcnt = lfcnt = lcnt = ofcnt = 0;
					
					oid = oidseq.nextValueLong();
					robso.getField(ofcnt++).setValue(oid);
					rlane.getField(lfcnt++).setValue(oid);
					
					for (String fval: input.split(","))
					{
						switch(fcnt)
						{
							// Time Stamp
							case 0:
							// Samples
							case 7:
							// % Observed
							case 8:
							// Total Flow
							case 9:
							// Avg Occupancy
							case 10:
							// Avg Speed
							case 11:
								rtype = "Observation";
								field = robso.getField(ofcnt++);
								break;
							// Station
							case 1:
							// District
							case 2:
							// Freeway #
							case 3:
							// Direction of Travel
							case 4:
							// Lane Type
							case 5:
							// Station Length
							case 6:
								rtype = "Station";
								field = rstation.getField(sfcnt++);
								break;
							default:
								rtype = "Lane";
								field = rlane.getField(lfcnt++);
								break;
						}
						
						if(fval == null || fval.isEmpty())
							fval = "0";
						
						outlog.log(Level.DEBUG, rtype + " - Field: " + fcnt + ", Type: " + field.getMetadata().getDataType().toString() + ", Val: '" + fval + "'");
						switch(field.getMetadata().getDataType())
						{
							case DATE:
								Date dval = format.parse(fval);
								field.setValue(dval);
								outlog.log(Level.DEBUG, "ParseDate: " + dval.toString() + " FieldDate: " + field.getValue().toString());
								break;
							case NUMBER:
								double nval = Double.parseDouble(fval);
								field.setValue(nval);
								break;
							case INTEGER:
								int ival = Integer.parseInt(fval);
								field.setValue(ival);
								break;
							case LONG:
								long lval = Long.parseLong(fval);
								field.setValue(lval);
								break;
							default:
								field.setValue(fval);
						}
						
						// Station Field needs to go into multiple tables
						if(fcnt == 1)
						{
							sid = (long)field.getValue();
							robso.getField(ofcnt++).setValue(sid);
							rlane.getField(lfcnt++).setValue(sid);
						}
	
						if(sfcnt == rstation.getNumFields())
						{
							writeRecordToPort(0, rstation);
							rstation.reset();
							sfcnt = 0;
						}
						
						if((lfcnt+1) == rlane.getNumFields())
						{
							rlane.getField(lfcnt).setValue(lcnt++);
							writeRecordToPort(1, rlane);
							rlane.reset();
							
							lfcnt = 0;
							rlane.getField(lfcnt++).setValue(oid);
							rlane.getField(lfcnt++).setValue(sid);
						}
						
						if(ofcnt == robso.getNumFields())
						{
							writeRecordToPort(2, robso);
							robso.reset();
							ofcnt = 0;
						}
						
						fcnt++;
					}
					rlane.reset();
				}
			}
			catch (IOException | ParseException e)
			{
				throw new JetelRuntimeException(e);
			}
		}
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		/** This way you can check connected edges and their metadata. */
		
		if (getComponent().getOutPorts().size() < 3) {
			status.add("Three Output port must be connected!", Severity.ERROR, getComponent(), Priority.NORMAL);
			return status;
		}
		
		DataRecordMetadata outMetadata0 = getComponent().getOutputPort(0).getMetadata();
		if (outMetadata0 == null) {
			status.add("Metadata on output port 0 not specified!", Severity.ERROR, getComponent(), Priority.NORMAL);
			return status;
		}
		
		DataRecordMetadata outMetadata1 = getComponent().getOutputPort(1).getMetadata();
		if (outMetadata1 == null) {
			status.add("Metadata on output port 1 not specified!", Severity.ERROR, getComponent(), Priority.NORMAL);
			return status;
		}

		DataRecordMetadata outMetadata2 = getComponent().getOutputPort(2).getMetadata();
		if (outMetadata2 == null) {
			status.add("Metadata on output port 2 not specified!", Severity.ERROR, getComponent(), Priority.NORMAL);
			return status;
		}

		return status;
	}

	@Override
	public void init() {
		super.init();
	}

	@Override
	public void preExecute() throws ComponentNotReadyException {
		super.preExecute();
	}

	@Override
	public void postExecute() throws ComponentNotReadyException {
		super.postExecute();
	}
}
