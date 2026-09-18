package com.qbe.kafkastarter.utils;

import static org.junit.jupiter.api.Assertions.*;

import com.qbe.avro.Marketing;
import com.qbe.avro.Price;
import com.qbe.avro.Product;
import com.qbe.avro.ProductMaster;
import com.qbe.avro.Stock;
import com.qbe.avro.Supplier;
import java.time.Instant;
import java.util.List;
import org.junit.jupiter.api.Test;

class TestEnricherUtils {

    @Test
    void shouldCreateProductMasterFromProductAndPrice() {
        Product product = Product.newBuilder()
                .setProductId("P001")
                .setSku("KB001")
                .setName("Keyboard")
                .setBrand("Microsoft")
                .setCategory("Accessories")
                .setActive(true)
                .setAlertThreshold(45)
                .setCreatedAt(Instant.parse("2026-08-26T09:00:00Z"))
                .build();

        Price price = Price.newBuilder()
                .setProductId("P001")
                .setAmount(49.99)
                .setCurrency("EUR")
                .setLastUpdatedAt(Instant.parse("2026-08-26T09:01:00Z"))
                .build();

        ProductMaster master = EnricherUtils.from(product, price);

        assertEquals("P001", master.getProductId());
        assertEquals("KB001", master.getSku());
        assertEquals("Keyboard", master.getName());
        assertEquals(45, master.getAlertThreshold());

        assertEquals(49.99, master.getPrice());
        assertEquals("EUR", master.getCurrency());
        assertEquals(Instant.parse("2026-08-26T09:01:00Z"), master.getLastUpdatedAt());
    }

    @Test
    void shouldCreateProductMasterWithoutPrice() {
        Product product = Product.newBuilder()
                .setProductId("P001")
                .setSku("KB001")
                .setName("Keyboard")
                .setBrand("Microsoft")
                .setCategory("Accessories")
                .setActive(true)
                .setCreatedAt(Instant.parse("2026-08-26T09:00:00Z"))
                .build();

        ProductMaster master = EnricherUtils.from(product, null);

        assertEquals(0d, master.getPrice());
        assertNull(master.getCurrency());
        assertNull(master.getLastUpdatedAt());
    }

    @Test
    void shouldEnrichWithStock() {
        ProductMaster master = buildProductMaster();

        Stock stock = Stock.newBuilder()
                .setProductId("P001")
                .setAvailableQuantity(120)
                .setReservedQuantity(10)
                .setLastUpdatedAt(Instant.parse("2026-08-26T09:02:00Z"))
                .setWarehouseCode("W001")
                .build();

        ProductMaster enriched = EnricherUtils.enrichWithStock(master, stock);

        assertEquals(120, enriched.getAvailableStock());
        assertEquals(10, enriched.getReservedStock());
        assertEquals(Instant.parse("2026-08-26T09:02:00Z"), enriched.getLastUpdatedAt());
    }

    @Test
    void shouldReturnSameMasterWhenStockIsNull() {
        ProductMaster master = buildProductMaster();
        ProductMaster enriched = EnricherUtils.enrichWithStock(master, null);

        assertSame(master, enriched);
    }

    @Test
    void shouldEnrichWithSupplier() {
        ProductMaster master = buildProductMaster();

        Supplier supplier = Supplier.newBuilder()
                .setProductId("P001")
                .setSupplierId("SUP001")
                .setSupplierName("TechSupplier")
                .setSupplierCountry("France")
                .setLastUpdatedAt(Instant.parse("2026-08-26T09:03:00Z"))
                .build();

        ProductMaster enriched = EnricherUtils.enrichWithSupplier(master, supplier);

        assertEquals("SUP001", enriched.getSupplierId());
        assertEquals("TechSupplier", enriched.getSupplierName());
        assertEquals("France", enriched.getSupplierCountry());
    }

    @Test
    void shouldReturnSameMasterWhenSupplierIsNull() {
        ProductMaster master = buildProductMaster();
        ProductMaster enriched = EnricherUtils.enrichWithSupplier(master, null);

        assertSame(master, enriched);
    }

    @Test
    void shouldEnrichWithMarketing() {
        ProductMaster master = buildProductMaster();

        Marketing marketing = Marketing.newBuilder()
                .setProductId("P001")
                .setShortDescription("Wireless Keyboard")
                .setLongDescription("Ergonomic wireless keyboard")
                .setTags(List.of("office", "wireless"))
                .setLastUpdatedAt(Instant.parse("2026-08-26T09:04:00Z"))
                .build();

        ProductMaster enriched = EnricherUtils.enrichWithMarketing(master, marketing);

        assertEquals("Wireless Keyboard", enriched.getShortDescription());
        assertEquals("Ergonomic wireless keyboard", enriched.getLongDescription());
        assertEquals(2, enriched.getTags().size());
    }

    @Test
    void shouldReturnSameMasterWhenMarketingIsNull() {
        ProductMaster master = buildProductMaster();
        ProductMaster enriched = EnricherUtils.enrichWithMarketing(master, null);

        assertSame(master, enriched);
    }

    private ProductMaster buildProductMaster() {
        return ProductMaster.newBuilder()
                .setProductId("P001")
                .setSku("KB001")
                .setName("Keyboard")
                .setBrand("Microsoft")
                .setCategory("Accessories")
                .setActive(true)
                .setAlertThreshold(40)
                .setCreatedAt(Instant.parse("2026-08-26T09:00:00Z"))
                .build();
    }
}
