package com.qbe.kafkastarter.mapper;

import com.qbe.avro.Supplier;
import com.qbe.kafkastarter.dto.SupplierDto;
import org.mapstruct.Mapper;

@Mapper(componentModel = "spring")
public interface SupplierMapper {

    Supplier toAvro(SupplierDto dto);

    SupplierDto fromAvro(Supplier event);
}
