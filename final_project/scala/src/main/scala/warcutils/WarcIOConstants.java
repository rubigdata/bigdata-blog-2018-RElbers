/**
 * Copyright 2014 SURFsara
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package warcutils;

import org.jwat.common.UriProfile;

/**
 * Constants used in this library by the reader and writer from the Java Web
 * Archive Toolkit.
 * 
 * @author mathijs.kattenberg@surfsara.nl
 */
public final class WarcIOConstants {

	public static final UriProfile URIPROFILE = UriProfile.RFC3986;
	public static final boolean BLOCKDIGESTENABLED = true;
	public static final boolean PAYLOADDIGESTENABLED = true;
	public static final int HEADERMAXSIZE = 8192;
	public static final int PAYLOADHEADERMAXSIZE = 32768;

	private WarcIOConstants() {
		// No instantiations
	}

}
