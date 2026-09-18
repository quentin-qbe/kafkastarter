package com.qbe.kafkastarter.utils;

import com.qbe.avro.*;

public final class EnricherUtils {

    private EnricherUtils() {}

    public static ProductMaster from(Product product, Price price) {
        return ProductMaster.newBuilder()
                .setProductId(product.getProductId())
                .setSku(product.getSku())
                .setName(product.getName())
                .setBrand(product.getBrand())
                .setCategory(product.getCategory())
                .setActive(product.getActive())
                .setAlertThreshold(product.getAlertThreshold())
                .setCreatedAt(product.getCreatedAt())
                .setPrice(price != null ? price.getAmount() : 0d)
                .setCurrency(price != null ? price.getCurrency() : null)
                .setLastUpdatedAt(price != null ? price.getLastUpdatedAt() : null)
                .build();
    }

    public static ProductMaster enrichWithStock(ProductMaster master, Stock stock) {
        if (stock == null) {
            return master;
        }

        return ProductMaster.newBuilder(master)
                .setAvailableStock(stock.getAvailableQuantity())
                .setReservedStock(stock.getReservedQuantity())
                .setLastUpdatedAt(stock.getLastUpdatedAt())
                .build();
    }

    public static ProductMaster enrichWithSupplier(ProductMaster master, Supplier supplier) {
        if (supplier == null) {
            return master;
        }

        return ProductMaster.newBuilder(master)
                .setSupplierId(supplier.getSupplierId())
                .setSupplierName(supplier.getSupplierName())
                .setSupplierCountry(supplier.getSupplierCountry())
                .setLastUpdatedAt(supplier.getLastUpdatedAt())
                .build();
    }

    public static ProductMaster enrichWithMarketing(ProductMaster master, Marketing marketing) {
        if (marketing == null) {
            return master;
        }

        return ProductMaster.newBuilder(master)
                .setShortDescription(marketing.getShortDescription())
                .setLongDescription(marketing.getLongDescription())
                .setTags(marketing.getTags())
                .setLastUpdatedAt(marketing.getLastUpdatedAt())
                .build();
    }
}
