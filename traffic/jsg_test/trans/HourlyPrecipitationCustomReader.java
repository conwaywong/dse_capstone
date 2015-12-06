import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;

import org.apache.log4j.Level;
import org.jetel.component.AbstractGenericTransform;
import org.jetel.data.DataRecord;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.JetelRuntimeException;

/**
 * This is an example custom reader. It shows how you can
 *  create records using a data source.
 */
public class HourlyPrecipitationCustomReader extends AbstractGenericTransform {

	@Override
	public void execute() {
		DataRecord record = outRecords[0];

		File path = getFile(getProperties().getStringProperty("FilesParentDir"));
		Collection<File> files = listFileTree(path);

		for (File file : files) {
			getLogger().log(Level.DEBUG, "Reading input: " + file.getPath());
			String str = null;
			try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
				while ((str = reader.readLine()) != null) {
					try {
						getLogger().log(Level.DEBUG, "Processing: " + str);
						int idx = 0;

						record.getField("record_type").setValue(str.substring(idx, idx+3));
						idx += 3;
						record.getField("state_code").setValue(str.substring(idx, idx+2));
						idx += 2;
						record.getField("cnin").setValue(str.substring(idx, idx+4));
						idx += 4;
						record.getField("cndn").setValue(str.substring(idx, idx+2));
						idx += 2;
						record.getField("element_type").setValue(str.substring(idx, idx+4));
						idx += 4;
						record.getField("element_units").setValue(str.substring(idx, idx+2));
						idx += 2;
						record.getField("year").setValue(str.substring(idx, idx+4));
						idx += 4;
						record.getField("month").setValue(str.substring(idx, idx+2));
						idx += 2;
						record.getField("day").setValue(str.substring(idx, idx+4));
						idx += 4;
						record.getField("num_reported_values").setValue(str.substring(idx, idx+3));
						idx += 3;
						record.getField("values").setValue(str.substring(idx));
						writeRecordToPort(0, record);
						record.reset();
					} catch (Exception e) {
						getLogger().log(Level.ERROR, "Invalid line: " + str);
					}
				}

			} catch (IOException e) {
				throw new JetelRuntimeException(e);

			}
		}
	}
	
	public Collection<File> listFileTree(File dir) {
		Set<File> fileTree = new HashSet<File>();

		for (File entry : dir.listFiles()) {
			if (entry.isFile()) fileTree.add(entry);
			else fileTree.addAll(listFileTree(entry));
		}
		return fileTree;
	}

	@Override
	public ConfigurationStatus checkConfig(ConfigurationStatus status) {
		super.checkConfig(status);

		/** This way you can check connected edges and their metadata. */
		/*
		if (getComponent().getOutPorts().size() < 1) {
			status.add("Output port must be connected!", Severity.ERROR, getComponent(), Priority.NORMAL);
			return status;
		}

		DataRecordMetadata outMetadata = getComponent().getOutputPort(0).getMetadata();
		if (outMetadata == null) {
			status.add("Metadata on output port not specified!", Severity.ERROR, getComponent(), Priority.NORMAL);
			return status;
		}

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
