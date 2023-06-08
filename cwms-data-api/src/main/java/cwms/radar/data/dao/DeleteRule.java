/*
 * MIT License
 *
 * Copyright (c) 2023 Hydrologic Engineering Center
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in all
 * copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 */

package cwms.radar.data.dao;

public enum DeleteRule
{
	// Not obvious which of the constants in CWMS_UTIL_PACKAGE are Delete rule and which aren't.
	DELETE_INSERT("DELETE INSERT"),
	DELETE_ALL("DELETE ALL"),
	DELETE_DATA("DELETE DATA"),
	DELETE_KEY("DELETE KEY"),
	DELETE_LOC("DELETE LOC"),
	DELETE_LOC_CASCADE("DELETE LOC CASCADE"),
	DELETE_TS_ID("DELETE TS ID"),
	DELETE_TS_DATA("DELETE TS DATA"),
	DELETE_TS_CASCADE("DELETE TS CASCADE")
	;

	private final String rule;

	DeleteRule(String rule)
	{
		this.rule = rule;
	}

	public String getRule()
	{
		return rule;
	}
}
