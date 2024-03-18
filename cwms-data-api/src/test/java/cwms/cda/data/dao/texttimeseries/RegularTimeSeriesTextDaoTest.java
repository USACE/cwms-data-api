package cwms.cda.data.dao.texttimeseries;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Test;

class RegularTimeSeriesTextDaoTest {

    @Test
    void sanitizeFilename() {
        assertEquals("myFile.txt", RegularTimeSeriesTextDao.sanitizeFilename("myFile.txt"));
        assertEquals("a.txt", RegularTimeSeriesTextDao.sanitizeFilename("a"));
        assertEquals("_TIME_SERIES_TEXT_6261044.txt", RegularTimeSeriesTextDao.sanitizeFilename("/TIME SERIES TEXT/6261044"));
    }

    @Test
    void sanitizeFilenameUnknown() {
        assertEquals("unknown.txt", RegularTimeSeriesTextDao.sanitizeFilename(""));
        assertEquals("unknown.txt", RegularTimeSeriesTextDao.sanitizeFilename(" "));
        assertEquals("unknown.txt", RegularTimeSeriesTextDao.sanitizeFilename("."));
        assertEquals("unknown.txt", RegularTimeSeriesTextDao.sanitizeFilename(".txt"));
        assertEquals("unknown.txt", RegularTimeSeriesTextDao.sanitizeFilename("unknown.txt"));
        assertEquals("unknown.txt", RegularTimeSeriesTextDao.sanitizeFilename(null));
    }
    @Test
    void sanitizeFilenameLeadingDot() {
        assertEquals(".bash.txt", RegularTimeSeriesTextDao.sanitizeFilename(".bash"));
        assertEquals(".profile.txt", RegularTimeSeriesTextDao.sanitizeFilename(".profile"));
        assertEquals(".._a.txt", RegularTimeSeriesTextDao.sanitizeFilename("../a.txt"));
    }

    @Test
    void sanitizeFilenameSpaces() {
        assertEquals("file_name_with_space.txt", RegularTimeSeriesTextDao.sanitizeFilename("file name with space"));
        assertEquals("a.dot.txt", RegularTimeSeriesTextDao.sanitizeFilename(" a.dot"));
    }

    @Test
    void sanitizeFilenameWeirdChars() {
        assertEquals("file_name_with___special_chars.txt", RegularTimeSeriesTextDao.sanitizeFilename("file_name_with_#_special_chars"));
        assertEquals("file_name_with___tab.txt", RegularTimeSeriesTextDao.sanitizeFilename("file_name_with_\t_tab"));
        assertEquals("file_name_with___newline.txt", RegularTimeSeriesTextDao.sanitizeFilename("file_name_with_\n_newline"));
        assertEquals("file_name_with___carriage_return.txt", RegularTimeSeriesTextDao.sanitizeFilename("file_name_with_\r_carriage_return"));
        assertEquals("file_name_with___backslash.txt", RegularTimeSeriesTextDao.sanitizeFilename("file_name_with_\\_backslash"));
        assertEquals("file_name_with___forwardslash.txt", RegularTimeSeriesTextDao.sanitizeFilename("file_name_with_/_forwardslash"));
        assertEquals("file_name_with___colon.txt", RegularTimeSeriesTextDao.sanitizeFilename("file_name_with_:_colon"));
        assertEquals("file_name_with___asterisk.txt", RegularTimeSeriesTextDao.sanitizeFilename("file_name_with_*_asterisk"));
        assertEquals("file_name_with___question_mark.txt", RegularTimeSeriesTextDao.sanitizeFilename("file_name_with_?_question_mark"));
        assertEquals("file_name_with___quote.txt", RegularTimeSeriesTextDao.sanitizeFilename("file_name_with_'_quote"));
        assertEquals("file_name_with___less_than.txt", RegularTimeSeriesTextDao.sanitizeFilename("file_name_with_<_less_than"));
        assertEquals("file_name_with___greater_than.txt", RegularTimeSeriesTextDao.sanitizeFilename("file_name_with_>_greater_than"));
        assertEquals("file_name_with___pipe.txt", RegularTimeSeriesTextDao.sanitizeFilename("file_name_with_|_pipe"));
        assertEquals("file_name_with___double_quote.txt", RegularTimeSeriesTextDao.sanitizeFilename("file_name_with_\"_double_quote"));
        assertEquals("file_name_with___semicolon.txt", RegularTimeSeriesTextDao.sanitizeFilename("file_name_with_;_semicolon"));
        assertEquals("file_name_with___equals.txt", RegularTimeSeriesTextDao.sanitizeFilename("file_name_with_=_equals"));
        assertEquals("file_name_with___comma.txt", RegularTimeSeriesTextDao.sanitizeFilename("file_name_with_,_comma"));
        assertEquals("file_name_with___space.txt", RegularTimeSeriesTextDao.sanitizeFilename("file_name_with_ _space"));
        assertEquals("file_name_with___plus.txt", RegularTimeSeriesTextDao.sanitizeFilename("file_name_with_+_plus"));
        assertEquals("file_name_with___at.txt", RegularTimeSeriesTextDao.sanitizeFilename("file_name_with_@_at"));

        // Think we'll allow these
        assertEquals("file_name_with__underscore.txt", RegularTimeSeriesTextDao.sanitizeFilename("file_name_with__underscore"));
        assertEquals("file_name_with_._period.txt", RegularTimeSeriesTextDao.sanitizeFilename("file_name_with_._period"));
        assertEquals("file_name_with_-_hyphen.txt", RegularTimeSeriesTextDao.sanitizeFilename("file_name_with_-_hyphen"));
    }
}