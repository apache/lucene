/* Copyright 2020 UPMEM. All rights reserved.
 * Use of this source code is governed by a BSD-style license that can be
 * found in the LICENSE file.
 */

package org.apache.lucene.sandbox.sdk;

import java.util.Map;

/**
 * The information from a compiled DPU program.
 *
 * @see DpuSymbol
 */
public final class DpuProgramInfo {
    private final Map<String, DpuSymbol> symbols;

    DpuProgramInfo(Map<String, DpuSymbol> symbols) {
        this.symbols = symbols;
    }

    /**
     * Get the DPU symbol with the given name.
     *
     * @param symbolName The DPU symbol name.
     * @return The DPU symbol for this name.
     * @exception DpuException When there is no symbol with the given name.
     * @see DpuSymbol
     * @see DpuException
     */
    public DpuSymbol get(String symbolName) throws DpuException {
        DpuSymbol symbol = this.symbols.get(symbolName);

        if (symbol == null) {
            throw new DpuException("unknown symbol '" + symbolName + "'");
        }

        return symbol;
    }
}
