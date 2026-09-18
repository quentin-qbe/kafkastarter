package com.qbe.kafkastarter.mapper;

import com.qbe.avro.ProductMaster;
import com.qbe.kafkastarter.dto.ProductMasterDto;
import java.math.BigDecimal;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface ProductMasterMapper {

    ProductMasterDto fromAvro(ProductMaster event);

    ProductMaster toAvro(ProductMasterDto dto);

    default Double map(BigDecimal value) {
        return value != null ? value.doubleValue() : null;
    }

    default BigDecimal map(Double value) {
        return value != null ? BigDecimal.valueOf(value) : null;
    }
}
