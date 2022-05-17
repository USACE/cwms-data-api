package cwms.radar.api;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import io.javalin.core.validation.Validator;
import org.apache.poi.ss.formula.functions.T;

import static com.codahale.metrics.MetricRegistry.name;

public class Controllers
{
	private Controllers(){

	}

	public static Timer.Context markAndTime(MetricRegistry registry, String className, String subject)
	{
		Meter meter = registry.meter(name(className,subject,"count"));
		meter.mark();
		Timer timer = registry.timer(name(className,subject,"time"));
		return timer.time();
	}

	public static <T> T queryParamAsClass(io.javalin.http.Context ctx, String[] names, Class<T> clazz, T defaultValue){
		T retval = defaultValue;

		Validator<T> validator = ctx.queryParamAsClass(names[0], clazz);
		if(validator.hasValue())
		{
			retval = validator.get();
		}
		else
		{
			for(int i = 1; i < names.length; i++)
			{
				validator = ctx.queryParamAsClass(names[0], clazz);
				if(validator.hasValue())
				{
					retval = validator.get();
					break;
				}
			}

		}

		return retval;
	}


	public static <T> T queryParamAsClass(io.javalin.http.Context ctx, String[] names, Class<T> clazz, T defaultValue,
	                                      MetricRegistry metrics, String className ){
		T retval = null;

		Validator<T> validator = ctx.queryParamAsClass(names[0], clazz);
		if(validator.hasValue())
		{
			retval = validator.get();
			metrics.counter(name(className,"correct")).inc();
		}
		else
		{
			for(int i = 1; i < names.length; i++)
			{
				validator = ctx.queryParamAsClass(names[0], clazz);
				if(validator.hasValue())
				{
					retval = validator.get();
					metrics.counter(name(className,"deprecated")).inc();
					break;
				}
			}

			if(retval == null){
				retval = defaultValue;
				metrics.counter(name(className,"default")).inc();
			}

		}

		return retval;
	}

}
