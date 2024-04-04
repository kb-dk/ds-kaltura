package dk.kb.kaltura;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.stubbing.Answer;

import static org.mockito.Mockito.anyInt;

/*
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
class MainTest {

    @BeforeAll
    static void setConfig() throws Exception {
        System.setProperty("dk.kb.applicationConfig", "src/main/conf/ds-kaltura-*.yaml");
    }

    /**
     * Check that the exit code signals success (code 0) when the input is valid.
     */
    @Test
    void acceptableInput() {
        assertExit(0, () -> {
            Main.main("Hansen", "87");
        });
    }

    /**
     * Check that the exit code signals fail (in this case code 2) when the input is invalid.
     */
    @Test
    void nonNumericAge() {
        assertExit(2, () -> {
            Main.main("Hansen", "otte");
        });
    }

    /**
     * Check that the exit code signals fail (in this case code 2) when there is no input.
     */
    @Test
    void nonInput() {
        assertExit(2, Main::main);
    }

    void assertExit(int expectedExitCode, Runnable runnable) {
        // Installs a static mock of the inner class SystemControl that temporarily disables the System.exit
        try (MockedStatic<Main.SystemControl> systemMock = Mockito.mockStatic(Main.SystemControl.class)) {
            systemMock.when(() -> Main.SystemControl.exit(anyInt())).thenAnswer((Answer<Void>) invocation -> null);
            runnable.run();
            systemMock.verify(() -> Main.SystemControl.exit(expectedExitCode));
        }
    }

}