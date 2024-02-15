package cwms.cda.data.dto.binarytimeseries;

import hec.data.timeSeriesText.DateDateKey;
import java.util.Comparator;
import java.util.Date;

/**
 *  The hec.data version of this class isn't null friendly.
 */
public class DateDateComparator implements Comparator<DateDateKey>
{

    @Override
    public int compare(DateDateKey o1, DateDateKey o2)
    {
       int compareTo;
       if(o1 == null && o2 == null)
       {
           compareTo = 0;
       } else if (o1 == null)
       {
           compareTo = -1;
       } else if (o2 == null)
       {
           compareTo = 1;
       } else {
              Date d1 = o1.getDateTime();
              Date d2 = o2.getDateTime();
              compareTo = compare(d1, d2);

              if(compareTo != 0)
              {
                  Date dataEntryDate1 = o1.getDataEntryDate();
                  Date dataEntryDate2 = o2.getDataEntryDate();
                  compareTo = compare(dataEntryDate1, dataEntryDate2);
              }
       }
       return compareTo;
    }

    public int compare(Date o1, Date o2){
        if(o1 == null && o2 == null)
        {
            return 0;
        } else if (o1 == null)
        {
            return -1;
        } else if (o2 == null)
        {
            return 1;
        } else {
            return o1.compareTo(o2);
        }
    }

}