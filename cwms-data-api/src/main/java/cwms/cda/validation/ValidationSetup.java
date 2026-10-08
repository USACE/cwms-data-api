package cwms.cda.validation;

import cwms.cda.api.Controllers;
import cwms.cda.api.enums.UnitSystem;
import cwms.cda.api.enums.VersionType;
import cwms.cda.api.rating.RatingController;
import cwms.cda.data.dao.JooqDao;
import cwms.cda.data.dao.StoreRule;
import cwms.cda.data.dao.VerticalDatum;
import cwms.cda.data.dto.watersupply.PumpType;
import hec.data.cwmsRating.RatingSet;
import io.javalin.config.ValidationConfig;

public final class ValidationSetup {

    private ValidationSetup() {
        /* utility class */
    }

    public static void registerValidation(ValidationConfig validation) {
        validation.register(UnitSystem.class, UnitSystem::systemFor);
        validation.register(JooqDao.DeleteMethod.class, Controllers::getDeleteMethod);
        validation.register(VersionType.class, VersionType::versionTypeFor);
        validation.register(StoreRule.class, StoreRule::getStoreRule);
        validation.register(VerticalDatum.class, VerticalDatum::getVerticalDatum);
        validation.register(RatingSet.DatabaseLoadMethod.class, RatingController::getDatabaseLoadMethod);
        validation.register(PumpType.class, PumpType::valueOf);
    }
}
