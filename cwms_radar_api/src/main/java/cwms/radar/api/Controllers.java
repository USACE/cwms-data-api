package cwms.radar.api;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;

import static com.codahale.metrics.MetricRegistry.name;

public class Controllers
{
	public static Timer.Context markAndTime(MetricRegistry registry, String className, String subject)
	{
		Meter meter = registry.meter(name(className,subject,"count"));
		meter.mark();
		Timer timer = registry.timer(name(className,subject,"time"));
		return timer.time();
	}
}
