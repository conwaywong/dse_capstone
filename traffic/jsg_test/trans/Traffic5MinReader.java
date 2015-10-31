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

		int rcnt = 0, tcnt, scnt, lcnt, ocnt;
		long oid;

		DataField field;

		outlog.log(Level.INFO, "Record0 NumFields: " + rstation.getNumFields());
		outlog.log(Level.INFO, "Record1 NumFields: " + rlane.getNumFields());
		outlog.log(Level.INFO, "Record2 NumFields: " + robso.getNumFields());
		outlog.log(Level.INFO, "InputFolder = " + folder.getName());
		outlog.log(Level.INFO, "SeqName = " + oidseq.getName());
		
		for(final File file : folder.listFiles())
		{
			if(file.isDirectory())
				continue;
			outlog.log(Level.INFO, "Inputfile = " + file.getName());
			try (BufferedReader br = new BufferedReader(new FileReader(file)))
			{
				while((input = br.readLine()) != null)// && rcnt < 3)
				{
					rcnt++;
					
					tcnt = scnt = lcnt = ocnt = 0;
					
					oid = oidseq.nextValueLong();
					robso.getField(0).setValue(oid);
					rlane.getField(0).setValue(oid);
					ocnt++; lcnt++;
					
					for (String fval: input.split(","))
					{
						switch(tcnt)
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
								field = robso.getField(ocnt);
								ocnt++;
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
								field = rstation.getField(scnt);
								scnt++;
								break;
							default:
								rtype = "Lane";
								field = rlane.getField(lcnt);
								lcnt++;
								break;
						}
						
						if(fval == null || fval.isEmpty())
							fval = "0";
						
						outlog.log(Level.DEBUG, rtype + " - Field: " + tcnt + ", Type: " + field.getMetadata().getDataType().toString() + ", Val: '" + fval + "'");
						switch(field.getMetadata().getDataType())
						{
							case DATE:
								Date dval = format.parse(fval);
								field.setValue(dval);
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
						// Station Field needs to go into two tables
						if(tcnt == 1)
						{
							robso.getField(ocnt).setValue(field.getValue());
							ocnt++;
						}
	
						if(scnt == rstation.getNumFields())
						{
							writeRecordToPort(0, rstation);
							rstation.reset();
							scnt = 0;
						}
						
						if(lcnt == rlane.getNumFields())
						{
							writeRecordToPort(1, rlane);
							rlane.reset();
							rlane.getField(0).setValue(oid);
							lcnt = 1;
						}
						
						if(ocnt == robso.getNumFields())
						{
	
							writeRecordToPort(2, robso);
							robso.reset();
							ocnt = 1;
						}
						
						tcnt++;
					}
					rlane.reset();
				}
			} catch (IOException | ParseException e)
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
			status.add("Two Output port must be connected!", Severity.ERROR, getComponent(), Priority.NORMAL);
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

		DataRecordMetadata outMetadata2 = getComponent().getOutputPort(1).getMetadata();
		if (outMetadata2 == null) {
			status.add("Metadata on output port 2 not specified!", Severity.ERROR, getComponent(), Priority.NORMAL);
			return status;
		}
		
		/*
		if (outMetadata.getFieldPosition("myMetadataFieldName") == -1) {
			status.add("Incompatible output metadata!", Severity.ERROR, getComponent(), Priority.NORMAL);
		}
		*/
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
