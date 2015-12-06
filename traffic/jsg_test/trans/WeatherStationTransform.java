import org.apache.log4j.Logger;
import org.jetel.component.AbstractGenericTransform;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.exception.JetelRuntimeException;
import org.jetel.metadata.DataRecordMetadata;

/**
 * 
 * @author kdyer
 *
 */
public class WeatherStationTransform extends AbstractGenericTransform {

	@Override
	public void execute() throws Exception {
		Logger LOG = getLogger();
		try {
			DataRecordMetadata outMetadata = getComponent()
					.getOutMetadataArray()[0];
			DataRecord inputRecord;
			int i = 1;
			//
			while ((inputRecord = readRecordFromPort(0)) != null) {
				if (LOG.isDebugEnabled()) {
					LOG.debug("Processing row= " + i);
				}
				String coopid = inputRecord.getField("COOPID").toString();
				coopid = coopid.substring(2, coopid.length());
				String coopname = inputRecord.getField("COOPNAME").toString();
				String latdir = inputRecord.getField("LATDIR").toString();
				Long latd = (Long) inputRecord.getField("LAT_D").getValue();
				Long latm = (Long) inputRecord.getField("LAT_M").getValue();
				Long lats = (Long) inputRecord.getField("LAT_S").getValue();
				String londir = inputRecord.getField("LONDIR").toString();
				Long lond = (Long) inputRecord.getField("LON_D").getValue();
				Long lonm = (Long) inputRecord.getField("LON_M").getValue();
				Long lons = (Long) inputRecord.getField("LON_S").getValue();
				Long elground = (Long) inputRecord.getField("EL_GROUND")
						.getValue();
				//
				Integer id = Integer.parseInt(coopid);
				String name = coopname;
				Float latitude = dms2dd(latd, latm, lats, latdir);
				Float longitude = dms2dd(lond, lonm, lons, londir);
				Float elevation = elground.floatValue();
				//
				DataRecord record = DataRecordFactory.newRecord(outMetadata);
				record.getField("ID").setValue(id);
				record.getField("NAME").setValue(name);
				record.getField("LATITUDE").setValue(latitude);
				record.getField("LONGITUDE").setValue(longitude);
				record.getField("ELEVATION").setValue(elevation);
				//
				writeRecordToPort(0, record);
				i++;
			}
		} catch (Exception e) {
			if (e instanceof RuntimeException) {
				throw (RuntimeException) e;
			}
			throw new JetelRuntimeException(e);
		}
	}

	private float dms2dd(Long degrees, Long minutes, Long seconds,
			String direction) {
		float dd = degrees.floatValue() + minutes.floatValue() / 60f
				+ seconds.floatValue() / (60f * 60f);
		if ("-".equals(direction)) {
			dd *= -1f;
		}
		return dd;
	}
}
