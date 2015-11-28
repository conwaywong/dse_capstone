//import org.apache.log4j.Level;
//import org.apache.log4j.Logger;
import org.jetel.component.AbstractGenericTransform;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.ConfigurationStatus.Priority;
import org.jetel.exception.ConfigurationStatus.Severity;
import org.jetel.metadata.DataRecordMetadata;

import java.util.*;

class DateCmp implements Comparator<DataRecord>
{
	public int compare(DataRecord d1, DataRecord d2) 
	{
		if(d2.getField("Eff_End").getValue() == null)
			return 1;

		if(d1.getField("Eff_End").getValue() == null)
			return -1;
		
		return ((Date)d1.getField("Eff_End").getValue()).compareTo((Date)d2.getField("Eff_End").getValue());
	}
}

public class DedupEffEnd extends AbstractGenericTransform 
{
	@Override
	public void execute() 
	{
		/*
		 * Metadata Fields: 
		 * 	PEMS_ID long
		 * 	Eff_Start date
		 * 	Eff_End date
		 */
		long curPEMSId = -1, newPEMSId = 0;
		Date curStartDate = null, newStartDate = null;
		List<DataRecord> curEndDates = new ArrayList<DataRecord>();

		/** Record prepared for reading from input port 0 */
		DataRecord inRecord = inRecords[0];

		/** Read all records from input port 0 */
		while ((inRecord = readRecordFromPort(0)) != null)
		{
			// Get values from input record
			newPEMSId = (long) inRecord.getField("PEMS_ID").getValue();
			newStartDate = (Date) inRecord.getField("Eff_Start").getValue();
			
			if(curPEMSId == -1)
			{
				curPEMSId = newPEMSId;
				curStartDate = (Date)newStartDate.clone();
			}

			/*
			 * Read in record, while the PEMS_ID and Eff_Start match save off Eff_End dates
			 * What ever the lowest eff_end date is (null is lowest) write that record out
			 */
			if(curPEMSId != newPEMSId || !curStartDate.equals(newStartDate))
			{
				Collections.sort(curEndDates, new DateCmp());
				
				while(curEndDates.size() > 1 && curEndDates.get(0).getField("Eff_End").getValue() == null)
					curEndDates.remove(0);
				
				writeRecordToPort(0, curEndDates.get(0));
				curEndDates.clear();
				
				// Update to this new one
				curPEMSId = newPEMSId;
				curStartDate = (Date)newStartDate.clone();;
				curEndDates.add(inRecord.duplicate());
			}
			else // both match, just add to list
				curEndDates.add(inRecord.duplicate());
		}
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status)
	{
		super.checkConfig(status);

		/** This way you can check connected edges and their metadata. */
		if (getComponent().getInPorts().size() < 1 || getComponent().getOutPorts().size() < 1)
		{
			status.add("Both input and output port must be connected!", Severity.ERROR, getComponent(), Priority.NORMAL);
			return status;
		}

		DataRecordMetadata inMetadata = getComponent().getInputPort(0).getMetadata();
		DataRecordMetadata outMetadata = getComponent().getOutputPort(0).getMetadata();
		if (inMetadata == null || outMetadata == null)
		{
			status.add("Metadata on input or output port not specified!", Severity.ERROR, getComponent(), Priority.NORMAL);
			return status;
		}
		
		if(inMetadata.getName() != outMetadata.getName())
		{
			status.add("Input and output Metadata must match!", Severity.ERROR, getComponent(), Priority.NORMAL);
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
