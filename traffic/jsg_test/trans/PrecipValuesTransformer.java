import org.apache.log4j.Level;
import org.jetel.component.AbstractGenericTransform;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;

/**
 * This is an example custom transformer. It shows how you can
 *  read records, process their values and write records.
 */
public class PrecipValuesTransformer extends AbstractGenericTransform {

	@Override
	public void execute() {

		/** Record prepared for reading from input port 0 */
		DataRecord inRecord = inRecords[0];

		/** Record prepared for writing to output port 0 */
		DataRecord outRecord = outRecords[0];

		/** Read all records from input port 0 */
		while ((inRecord = readRecordFromPort(0)) != null) {

			try {
				outRecord.getField("cnin").setValue(inRecord.getField("cnin").getValue());
				outRecord.getField("cndn").setValue(inRecord.getField("cndn").getValue());
				outRecord.getField("element_units").setValue(inRecord.getField("element_units").getValue());
				outRecord.getField("year").setValue(inRecord.getField("year").getValue());
				outRecord.getField("month").setValue(inRecord.getField("month").getValue());
				outRecord.getField("day").setValue(inRecord.getField("day").getValue());

				Integer numValues = (Integer) inRecord.getField("num_reported_values").getValue();
				String valuesStr = inRecord.getField("values").toString();

				String[] values = valuesStr.split(" +");
				if (values.length / 2 != numValues) {
					getLogger().log(Level.ERROR, String.format("Values list unexpected length.  Expected %d. Got %d.", 2 * numValues, values.length));
					continue;
				}

				//			getLogger().log(Level.INFO, values.toString());

				for (int i = 0; i < values.length; i += 2) {

					for (int f = 6; f < outRecord.getNumFields(); f++) {
						outRecord.reset(f);
					}

					outRecord.getField("hour").setValue(Integer.parseInt(values[i]) / 100);

					outRecord.getField("amount").setValue(Integer.parseInt((values[i+1]).substring(0, 5)));
					if ((values[i+1]).length() > 5) outRecord.getField("flag1").setValue((values[i+1]).substring(5,6));
					if ((values[i+1]).length() > 6) outRecord.getField("flag2").setValue((values[i+1]).substring(6,7));

					writeRecordToPort(0, outRecord);

				};
			} catch (Exception e) {
				getLogger().log(Level.WARN, e.getMessage());
			}



		}
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		/** This way you can check connected edges and their metadata. */
		/*
		if (getComponent().getInPorts().size() < 1 || getComponent().getOutPorts().size() < 1) {
			status.add("Both input and output port must be connected!", Severity.ERROR, getComponent(), Priority.NORMAL);
			return status;
		}

		DataRecordMetadata inMetadata = getComponent().getInputPort(0).getMetadata();
		DataRecordMetadata outMetadata = getComponent().getOutputPort(0).getMetadata();
		if (inMetadata == null || outMetadata == null) {
			status.add("Metadata on input or output port not specified!", Severity.ERROR, getComponent(), Priority.NORMAL);
			return status;
		}

		if (inMetadata.getFieldPosition("myIntegerField") == -1) {
			status.add("Incompatible input metadata!", Severity.ERROR, getComponent(), Priority.NORMAL);
		}
		if (outMetadata.getFieldPosition("myIntegerField") == -1) {
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
